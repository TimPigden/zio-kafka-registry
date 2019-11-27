package zio.kafka.registry

import zio.test._
import TestRestSupport._
import KafkaTestUtils._
import zio._
import zio.kafka.registry.rest.ConfluentClient
import zio.test.TestAspect._
import TestProducerSupport._
import zio.blocking.Blocking

object TestProducer extends DefaultRunnableSpec(
  suite("test producing with avro serializer")(
    testPresident
  )
.provideManagedShared(kafkaEnvironmentConfluent(Kafka.makeEmbedded)) @@ sequential
)


object TestProducerSupport{
  def restClientService =
    ZIO.environment[ConfluentClient[Blocking]].map {_.client}

  val subject = "presidents"

  val presidents = List(President2("Lincoln", 1860),
    President2("Obama", 2008),
    President2("Trump", 2016))

  val testPresident = testM("test define and store president") {
    for {
      restClient <- restClientService
//      _ <- restClient.registerSchema(subject, schema1)
      _ <- produceMany(subject, presidents.map ( p => "all" -> p))(format2)
      subjects <- restClient.subjects
    } yield {
      println(s"got these subjects $subjects")
      assertCompletes
    }
  }

}
