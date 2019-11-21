package zio.kafka.registry

import net.manub.embeddedkafka.schemaregistry.{EmbeddedKWithSR, EmbeddedKafka}
import org.apache.kafka.clients.consumer.ConsumerConfig
import zio.{Chunk, Managed, RIO, UIO, ZIO, ZManaged}
import org.apache.kafka.clients.producer.ProducerRecord
import zio.blocking.Blocking
import zio.clock.Clock
import zio.kafka.client.serde.Serde
import zio.duration._
import zio.kafka.client.AdminClient.KafkaAdminClientConfig
import zio.random.Random
import zio.test.environment.{Live, TestEnvironment}
import Kafka._
import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.generic.GenericRecord
import zio.kafka.client._
import zio.kafka.registry.rest.{RestClient, RestClientImpl}

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

  type KafkaTestEnvironment = Kafka with TestEnvironment with RestClient

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

  def kafkaEnvironment(kafkaE: Managed[Nothing, Kafka]): Managed[Throwable, KafkaTestEnvironment] =
    for {
      testEnvironment <- TestEnvironment.Value
      kafkaS          <- kafkaE
      http4sClient <-  Http4sClient.make
    } yield new TestEnvironment(
      testEnvironment.blocking,
      testEnvironment.clock,
      testEnvironment.console,
      testEnvironment.live,
      testEnvironment.random,
      testEnvironment.sized,
      testEnvironment.system
    ) with Kafka with RestClient {
      val kafka = kafkaS.kafka
      val restClient = RestClientImpl(Http4sClient(http4sClient, kafka.registryServer))
    }


  val embeddedKafkaEnvironment: Managed[Throwable, KafkaTestEnvironment] =
    kafkaEnvironment(Kafka.makeEmbedded)

  val localKafkaEnvironment: Managed[Throwable, KafkaTestEnvironment] =
    kafkaEnvironment(Kafka.makeLocal)

  def registryProducerSettings =
    for {
      servers <- ZIO.access[Kafka](_.kafka.bootstrapServers)
      registry <- ZIO.access[Kafka](_.kafka.registryServer)
    } yield ProducerSettings(
      servers,
      5.seconds,
      Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> registry)
    )

  def withProducer[A, K, V](
    r: Producer[Any, K, V] => RIO[Any with Clock with Kafka with Blocking, A],
    kSerde: Serde[Any, K],
    vSerde: Serde[Any, V]
  ): RIO[KafkaTestEnvironment, A] =
    for {
      settings <- registryProducerSettings
      producer = Producer.make(settings, kSerde, vSerde)
      lcb      <- Kafka.liveClockBlocking
      produced <- producer.use { p =>
                   r(p).provide(lcb)
                 }
    } yield produced

  def withProducerStringRecord[A, V](
                                      r: Producer[Any, String, GenericRecord] => RIO[Any with Clock with Kafka with Blocking, A],
                                      format: RecordFormat[V]
                        ) = withProducer[A, String, GenericRecord](Serde.string, Serde.g)

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
               .adminClient(settings)
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

}
