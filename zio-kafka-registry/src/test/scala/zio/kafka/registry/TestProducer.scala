package zio.kafka.registry

import zio.test._
import TestRestSupport._
import KafkaTestUtils._
import zio.ZIO
import zio.kafka.registry.rest.ConfluentClient
import zio.test.TestAspect._

object TestProducer extends DefaultRunnableSpec(
  suite("test producing with avro serializer")()
.provideManagedShared(kafkaEnvironmentConfluent(Kafka.makeEmbedded)) @@ sequential
)


object TestProducerSupport{
  def restClientService =
    ZIO.environment[ConfluentClient].map {_.client}

  val subject = "presidents"

  val presidents = List(President2("Lincoln", 1860),
    President2("Obama", 2008),
    President2("Trump", 2016))

  val testPresident = testM("test define and store president") {
    for {
      restClient <- restClientService
      posted <- restClient.registerSchema(subject, schema1)
      toRecord =



    } yield ???
  }

}
