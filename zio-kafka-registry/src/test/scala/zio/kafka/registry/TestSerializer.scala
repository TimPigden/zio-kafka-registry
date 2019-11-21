package zio.kafka.registry

import zio.kafka.registry.KafkaTestUtils.embeddedKafkaEnvironment
import zio.kafka.registry.TestSerializerSupport._
import zio.test._

object TestSerializer extends DefaultRunnableSpec(
  suite("test rest  interface")(

  ).provideManagedShared(embeddedKafkaEnvironment)
)

object TestSerializerSupport {

  val testProduceAutoCreatesSchema = testM("producing using an avro producer should write to schema"){
    for {

    }

  }

}
