package zio.kafka.registry

import zio.kafka.registry.KafkaRegistryTestUtils._
import zio.kafka.registry.TestRestSupport._
import zio.test._
import zio.test.TestAspect._

object TestRestConfluent extends DefaultRunnableSpec {
  def spec = suite("test rest  interface")(
    allTests: _*

  ).provideSomeManagedShared(embeddedConfluentKafkaEnvironment) @@ sequential
}
