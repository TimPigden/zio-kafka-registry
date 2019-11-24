package zio.kafka.registry

import zio.ZIO
import zio.kafka.registry.rest.RestClient
import zio.test._
import Assertion._
import TestRestSupport._
import KafkaTestUtils._
import com.sksamuel
import com.sksamuel.avro4s.AvroSchema
import zio.kafka.registry.rest.RestClient.{Backward, CompatibilityLevel}

object TestRest extends DefaultRunnableSpec(
  suite("test rest  interface")(
     testSubjects,
     testDelete,
     modifyCompatibility,
    checkDeleteSubject,
     multipleSchemas,
    compatibleSchemas,
  ).provideManagedShared(embeddedKafkaEnvironment)
)

object TestRestSupport {
  case class President1(name: String)
  case class President2(name: String, age: Int)


  val schema1 = AvroSchema[President1]
  val schema2 = AvroSchema[President2]

  val testSubjects = testM("test subjects empty then non-empty"){
    val subject = "presidents"
    for {
      rc <- ZIO.environment[RestClient]
      restClient = rc.restClient
      initial <- restClient.subjects
      posted <-  restClient.registerSchema(subject, schema1)
      already <- restClient.alreadyPresent(subject, schema1)
      schemaBack <- restClient.schema(posted)
      later <- restClient.subjects
    } yield {
      assert(initial, not(contains(subject))) &&
        assert(later, contains(subject)) &&
      assert(posted, equalTo(1)) &&
      assert(already,not (equalTo(None))) &&
      assert(already.get.schema, equalTo(schema1)) &&
        assert(schemaBack, equalTo(schema1))
    }
  }

  val testDelete = testM("add then delete"){
    val subject = "morepresidents"
    for {
      rc <- ZIO.environment[RestClient]
      restClient = rc.restClient
      initial <- restClient.subjects
      posted <-  restClient.registerSchema(subject, schema1)
      later <- restClient.subjects
      _ <- restClient.delete(subject, posted)
      laterStill <- restClient.subjects
    } yield {
      assert(initial, not(contains(subject))) &&
        assert(later, contains(subject)) &&
        assert(posted, equalTo(1)) &&
      assert(laterStill, not(contains(subject)))
    }
  }

  val checkDeleteSubject = testM("delete whole subject"){
    val subject = "morepresidents2"
    for {
      rc <- ZIO.environment[RestClient]
      restClient = rc.restClient
      posted <-  restClient.registerSchema(subject, schema1)
      later <- restClient.subjects
      _ <- restClient.deleteSubject(subject)
      laterStill <- restClient.subjects
    } yield {
        assert(later, contains(subject)) &&
        assert(posted, equalTo(1)) &&
        assert(laterStill, not(contains(subject)))
    }
  }

  val multipleSchemas = testM("test with multiple schemas in same subject"){
    val subject = "presidents3"
    for {
      rc <- ZIO.environment[RestClient]
      restClient = rc.restClient
      _ <-  restClient.registerSchema(subject, schema1)
      _ <-  restClient.registerSchema(subject, schema2)
      versions <- restClient.subjectVersions(subject)
    } yield {
      assert(versions, equalTo(List(1, 2)))
    }
  }

  val compatibleSchemas = testM("schema1 and 2 are compatible"){
    val subject = "presidents4"
    for {
      rc <- ZIO.environment[RestClient]
      restClient = rc.restClient
      _ <- restClient.registerSchema(subject, schema1)
      compat <- restClient.compatible(subject, 1, schema2)
    } yield {
      assert(compat, equalTo(true))
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
