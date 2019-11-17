package zio.kafka.registry

import zio.ZIO
import zio.avro.magnolia.{AvroCompiler, AvroSchemaDerivation}
import zio.kafka.registry.rest.RestClient
import zio.test._
import zio.avro.magnolia.SimpleSchemaGenerator._
import Assertion._
import TestRestSupport._
import KafkaTestUtils._
import zio.kafka.registry.rest.RestClient.CompatibilityLevel

object TestRest extends DefaultRunnableSpec(
  suite("test rest  interface")(
    testSubjects, testDelete, modifyCompatibility
  ).provideManagedShared(embeddedKafkaEnvironment)
)

object TestRestSupport {
  case class President1(name: String)
  case class President2(name: String, age: Int)


  val schema1 = AvroCompiler.compile(AvroSchemaDerivation.avroSchema[President1].generate)
  val schema2 = AvroCompiler.compile(AvroSchemaDerivation.avroSchema[President2].generate)

  val testSubjects = testM("test subjects empty then non-empty"){
    val subject = "presidents"
    for {
      rc <- ZIO.environment[RestClient]
      restClient = rc.restClient
      initial <- restClient.subjects
      posted <-  restClient.postSchema(subject, schema1)
      already <- restClient.alreadyPresent(subject, schema1)
      later <- restClient.subjects
    } yield {
      assert(initial, not(contains(subject))) &&
        assert(later, contains(subject)) &&
      assert(posted, equalTo(1)) &&
      assert(already,not (equalTo(None)))
      assert(already.get.schema, equalTo(schema1))
    }
  }

  val testDelete = testM("add then delete"){
    val subject = "morepresidents"
    for {
      rc <- ZIO.environment[RestClient]
      restClient = rc.restClient
      initial <- restClient.subjects
      posted <-  restClient.postSchema(subject, schema1)
      later <- restClient.subjects
      _ <- restClient.delete(subject, posted)
      laterStill <- restClient.subjects
    } yield {
      assert(initial, not(contains(subject))) &&
        assert(later, contains(subject)) &&
        assert(posted, equalTo(1)) &&
      assert(laterStill, equalTo(List.empty[String]))
    }
  }

  def checkAll(testResults: Iterable[TestResult]) =
    testResults.tail.foldLeft(testResults.head)((acc, it) => acc && it)

  val modifyCompatibility = testM("modify compatibility and check") {

    val subject = "presidents"
    def setCheck(restClient: RestClient.Service[Any], compat: CompatibilityLevel) =
      for {
        _ <- restClient.setConfig(compat)
        check <- restClient.config
      } yield assert(check, equalTo(compat))

    def setCheck2(restClient: RestClient.Service[Any], compat: CompatibilityLevel) =
      for {
        _ <- restClient.setConfig(subject, compat)
        check <- restClient.config(subject)
      } yield assert(check, equalTo(compat))

    for {
      rc <- ZIO.environment[RestClient]
      restClient = rc.restClient
      general <- ZIO.collectAll(CompatibilityLevel.values.keySet.map { compat => setCheck(restClient, compat)})
      bySubject<- ZIO.collectAll(CompatibilityLevel.values.keySet.map { compat => setCheck2(restClient, compat)})
    } yield {
      checkAll(general) &&
      checkAll(bySubject)
    }

  }

}
