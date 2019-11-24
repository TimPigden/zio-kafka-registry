package zio.kafka.registry

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import net.manub.embeddedkafka.avro.KafkaAvroSerializer
import zio._
import zio.kafka.registry.KafkaTestUtils.embeddedKafkaEnvironment
import zio.kafka.registry.TestSerializerSupport._
import zio.test._
import zio.blocking._
import zio.kafka.client.serde.Serializer
import zio.kafka.registry.Kafka.KafkaTestEnvironment

object TestSerializer extends DefaultRunnableSpec(
  suite("test rest  interface")(

  ).provideManagedShared(embeddedKafkaEnvironment)
)

object TestSerializerSupport {

/*
  def registrySerializer: RIO[KafkaTestEnvironment, Serializer] = effectBlocking{
    val registryClient = new CachedSchemaRegistryClient()
    val ka = new KafkaAvroSerializer[]


  }

  )

  val testProduceAutoCreatesSchema = testM("producing using an avro producer should write to schema"){
    for {


    }

  }
*/

}
