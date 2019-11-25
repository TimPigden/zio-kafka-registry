package zio.kafka.registry

import zio.ZIO
import zio.kafka.registry.rest.RestClient
import zio.test._
import Assertion._
import TestRestSupport._
import KafkaTestUtils._
import com.sksamuel
import com.sksamuel.avro4s.AvroSchema
import zio.blocking.Blocking
import zio.kafka.registry.rest.RestClient.{Backward, CompatibilityLevel}

/*
object TestRestHttp4s extends DefaultRunnableSpec(
  suite("test rest  interface")(
     testSubjects,
//     testDelete,
//     modifyCompatibility,
//    checkDeleteSubject,
//     multipleSchemas,
//    compatibleSchemas,
  ).provideManagedShared(embeddedHttp4sKafkaEnvironment)
)
*/
