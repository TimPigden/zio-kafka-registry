package zio.kafka.registry

import zio.test._
import TestRestSupport._
import KafkaRegistryTestUtils._
import zio._
import zio.test.TestAspect._
import TestProducerSupport._
import Assertion._
import zio.kafka.client.Subscription
import zio.kafka.client.serde.Serde
import zio.kafka.registry.Kafka.KafkaTestEnvironment
import zio.kafka.registry.Settings.RecordNameStrategy

object TestProducerConsumer extends DefaultRunnableSpec(
  suite("test producing with avro serializer")(
    testPresident
  )
.provideManagedShared(embeddedConfluentKafkaEnvironment) @@ sequential
)


object TestProducerSupport{
  val topic = "presidents"

  val presidents = List(President2("Lincoln", 1860),
    President2("Obama", 2008),
    President2("Trump", 2016))

  val testPresident: ZSpec[KafkaTestEnvironment, Throwable, String, Unit] = testM("test define and store president") {
    for {
      restClient <- ZIO.environment[ConfluentClient]
      _ <- produceMany(topic, presidents.map (p => "all" -> p))(format2)
      subjects <- restClient.confluentClient.subjects
      deserializer <- restClient.confluentClient.avroGenericDeserializer[President2](RecordNameStrategy)(format2)
      records <- withConsumer("any", "client") { consumer =>
        consumer
          .subscribeAnd(Subscription.Topics(Set(topic)))
          .plainStream(Serde.string, deserializer)
          .flattenChunks
          .take(3)
          .runCollect
      }
      vOut = records.map{_.record.value}
    } yield {
      println(s"got these subjects $subjects")
      assert(vOut, equalTo(presidents))
    }
  }

}
