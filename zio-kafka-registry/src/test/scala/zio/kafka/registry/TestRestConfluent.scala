package zio.kafka.registry

import zio.kafka.registry.KafkaTestUtils._
import zio.kafka.registry.TestRestSupport._
import zio.test._
import zio.test.TestAspect._

object TestRestConfluent extends DefaultRunnableSpec(
  suite("test rest  interface")(
    confluentAllTests.allTests :_*

  ).provideManagedShared(kafkaEnvironmentConfluent(Kafka.makeEmbedded)) @@ sequential
)
