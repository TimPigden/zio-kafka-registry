package zio.kafka.registry

import net.manub.embeddedkafka.schemaregistry.{EmbeddedKWithSR, EmbeddedKafka}
import org.apache.kafka.clients.consumer.ConsumerConfig
import zio.{Cause, Chunk, Managed, RIO, Semaphore, UIO, ZIO, ZManaged}
import org.apache.kafka.clients.producer.ProducerRecord
import zio.blocking.Blocking
import zio.clock.Clock
import zio.kafka.client.serde.{Serde, Serializer}
import zio.duration._
import zio.kafka.client.AdminClient.KafkaAdminClientConfig
import zio.random.Random
import zio.test.environment.{Live, TestEnvironment}
import Kafka._
import com.sksamuel.avro4s.{RecordFormat, ToRecord}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import zio.kafka.client._
import zio.kafka.registry.rest.{AbstractClient, ConfluentClient, RestClient}
import io.confluent.kafka.schemaregistry.client.rest.{RestService => ConfluentRestService}
import org.apache.avro.Schema
import zio.kafka.registry.Settings.RecordNameStrategy
import zio.test.TestFailure

trait Kafka {
  def kafka: Kafka.Service
}

object Kafka {
  trait Service {
    def bootstrapServers: List[String]
    def registryServer: String
    def stop(): UIO[Unit]
  }

  // documented constants (not sure how to get them programmatically)


  case class EmbeddedKafkaService(embeddedK: EmbeddedKWithSR) extends Kafka.Service {
    private val kafkaPort = 6001
    val schemaRegistryPort = 6002
    override def bootstrapServers: List[String] = List(s"localhost:$kafkaPort")
    override def registryServer: String = s"http://localhost:$schemaRegistryPort"
    override def stop(): UIO[Unit]              = ZIO.effectTotal(embeddedK.stop(true))
  }

  case object DefaultLocal extends Kafka.Service {
    override def bootstrapServers: List[String] = List(s"localhost:9092")
    override def registryServer: String = s"http://localhost:8081"

    override def stop(): UIO[Unit] = UIO.unit
  }

  val makeEmbedded: Managed[Nothing, Kafka] =
    ZManaged.make(ZIO.effectTotal(new Kafka {
      override val kafka: Service = EmbeddedKafkaService(EmbeddedKafka.start())
    }))(_.kafka.stop())

  val makeLocal: Managed[Nothing, Kafka] =
    ZManaged.make(ZIO.effectTotal(new Kafka {
      override val kafka: Service = DefaultLocal
    }))(_.kafka.stop())

  type KafkaTestEnvironment = Kafka with TestEnvironment

  type KafkaClockBlocking = Kafka with Clock with Blocking

  def liveClockBlocking: ZIO[KafkaTestEnvironment, Nothing, KafkaClockBlocking] =
    for {
      clck    <- Live.live(ZIO.environment[Clock])
      blcking <- ZIO.environment[Blocking]
      kfka    <- ZIO.environment[Kafka]
    } yield new Kafka with Clock with Blocking {
      override val kafka: Service = kfka.kafka

      override val clock: Clock.Service[Any]       = clck.clock
      override val blocking: Blocking.Service[Any] = blcking.blocking
    }

}

object KafkaTestUtils {

  def kafkaEnvironmentConfluent(kafkaE: Managed[Nothing, Kafka]) =
    for {
      testEnvironment <- TestEnvironment.Value
      kafkaS          <- kafkaE
      sem <- ZManaged.fromEffect(Semaphore.make(1L))
    } yield new TestEnvironment(
      testEnvironment.blocking,
      testEnvironment.clock,
      testEnvironment.console,
      testEnvironment.live,
      testEnvironment.random,
      testEnvironment.sized,
      testEnvironment.system
    ) with Kafka with ConfluentClient[Blocking] {
      val kafka = kafkaS.kafka
      val client = {
        val confluentService = new ConfluentRestService(kafka.registryServer)
        ConfluentClientImpl(confluentService, RecordNameStrategy, kafka.registryServer, sem).client
      }
    }

  val embeddedConfluentKafkaEnvironment =
    kafkaEnvironmentConfluent(Kafka.makeEmbedded)
      .mapErrorCause(cause => Cause.fail(TestFailure.Runtime(cause)))

  val localConfluentKafkaEnvironment =
    kafkaEnvironmentConfluent(Kafka.makeLocal)

  def registryProducerSettings =
    for {
      servers <- ZIO.access[Kafka](_.kafka.bootstrapServers)
      registry <- ZIO.access[Kafka](_.kafka.registryServer)
    } yield ProducerSettings(
      servers,
      5.seconds,
      Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> registry,
        AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY-> "io.confluent.kafka.serializers.subject.RecordNameStrategy")
    )

  def withProducer[A, K, V](    kSerde: Serializer[Any, K],
                                vSerde: Serializer[Any with Blocking, V]
                           )(r: Producer[Any with Blocking, K, V] => RIO[Any with Clock with Kafka with Blocking, A],
  ): RIO[KafkaTestEnvironment, A] =
    for {
      settings <- registryProducerSettings
      producer = Producer.make(settings, kSerde, vSerde)
      lcb      <- Kafka.liveClockBlocking
      produced <- producer.use { p =>
                   r(p).provide(lcb)
                 }
    } yield produced

  def withProducerAvroRecord[A, V](recordFormat: RecordFormat[V])
                                  (r: Producer[Any with Blocking, String, GenericRecord] => RIO[Any with Clock with Kafka with Blocking, A]
                                  ) = {
    for {
      serializer <-  ZIO.environment[ConfluentClient[Blocking]].map {_.client.avroSerializer}
      producing <-  withProducer[A, String, GenericRecord](Serde.string, serializer)(r)
    } yield producing
  }

  def consumerSettings(groupId: String, clientId: String) =
    for {
      servers <- ZIO.access[Kafka](_.kafka.bootstrapServers)
      registry <- ZIO.access[Kafka](_.kafka.registryServer)
    } yield ConsumerSettings(
      servers,
      groupId,
      clientId,
      5.seconds,
      Map(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ConsumerConfig.METADATA_MAX_AGE_CONFIG  -> "100",
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> registry
      ),
      250.millis,
      250.millis,
      1
    )

  def consumeWithStrings(groupId: String, clientId: String, subscription: Subscription)(
    r: (String, String) => ZIO[Any with Kafka with Clock with Blocking, Nothing, Unit]
  ): RIO[KafkaTestEnvironment, Unit] =
    for {
      lcb <- Kafka.liveClockBlocking
      inner <- (for {
                settings <- consumerSettings(groupId, clientId)
                consumed <- Consumer.consumeWith(settings, subscription, Serde.string, Serde.string)(r)
              } yield consumed)
                .provide(lcb)
    } yield inner

  def withConsumer[A](groupId: String, clientId: String)(
    r: Consumer => RIO[Any with Kafka with Clock with Blocking, A]
  ): RIO[KafkaTestEnvironment, A] =
    for {
      lcb <- Kafka.liveClockBlocking
      inner <- (for {
                settings <- consumerSettings(groupId, clientId)
                consumer = Consumer.make(settings)
                consumed <- consumer.use { p =>
                             r(p).provide(lcb)
                           }
              } yield consumed).provide(lcb)
    } yield inner

  def adminSettings =
    for {
      servers <- ZIO.access[Kafka](_.kafka.bootstrapServers)
    } yield KafkaAdminClientConfig(servers)

  def withAdmin[T](f: AdminClient => RIO[Any with Clock with Kafka with Blocking, T]) =
    for {
      settings <- adminSettings
      lcb      <- Kafka.liveClockBlocking
      fRes <- AdminClient
               .make(settings)
               .use { client =>
                 f(client)
               }
               .provide(lcb)
    } yield fRes

  // temporary workaround for zio issue #2166 - broken infinity
  val veryLongTime = Duration.fromNanos(Long.MaxValue)

  def randomThing(prefix: String) =
    for {
      random <- ZIO.environment[Random]
      l      <- random.random.nextLong(8)
    } yield s"$prefix-$l"

  def randomTopic = randomThing("topic")

  def randomGroup = randomThing("group")

  def produceMany[T](t: String, kvs: Iterable[(String, T)])
                    (implicit recordFormat: RecordFormat[T]) =
    withProducerAvroRecord(recordFormat) { p =>
      val records = kvs.map {
        case (k, v) =>
          val rec = recordFormat.to(v)
          new ProducerRecord[String, GenericRecord](t, k, rec)
      }
      val chunk = Chunk.fromIterable(records)
      p.produceChunk(chunk)
    }.flatten



}
