package zio.kafka.registry

import zio.ZIO
import zio.test._
import Assertion._
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import ConfluentRestService._

object TestRestSupport {
  case class President1(name: String)
  case class President2(name: String, age: Int)


  val schema1 = AvroSchema[President1]
  val schema2 = AvroSchema[President2]

  val format1 = RecordFormat[President1]
  val format2 = RecordFormat[President2]

    def allTests = List(
     testSubjects,
      testDelete,
      modifyCompatibility,
      checkDeleteSubject,
      multipleSchemas,
      compatibleSchemas
    )

    val testSubjects 
    = testM("test subjects empty then non-empty") {
      val subject = "presidents"
      for {
        restClient <- ZIO.environment[ConfluentClient].map(_.confluentClient)
        initial <- restClient.subjects
        posted <- restClient.registerSchema(subject, schema1)
        already <- restClient.alreadyPresent(subject, schema1)
        schemaBack <- restClient.schema(posted)
        later <- restClient.subjects
      } yield {
        assert(initial, not(contains(subject))) &&
          assert(later, contains(subject)) &&
          assert(posted, equalTo(1)) &&
          assert(already, not(equalTo(None))) &&
          assert(already.get.schema, equalTo(schema1)) &&
          assert(schemaBack, equalTo(schema1))
      }
    }

    val testDelete = testM("add then delete") {
      val subject = "morepresidents"
      for {
        restClient <- ZIO.environment[ConfluentClient].map(_.confluentClient)
        initial <- restClient.subjects
        posted <- restClient.registerSchema(subject, schema1)
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

    val checkDeleteSubject = testM("delete whole subject") {
      val subject = "morepresidents2"
      for {
        restClient <- ZIO.environment[ConfluentClient].map(_.confluentClient)
        posted <- restClient.registerSchema(subject, schema1)
        later <- restClient.subjects
        _ <- restClient.deleteSubject(subject)
        laterStill <- restClient.subjects
      } yield {
        assert(later, contains(subject)) &&
          assert(posted, equalTo(1)) &&
          assert(laterStill, not(contains(subject)))
      }
    }

    val multipleSchemas = testM("test with multiple schemas in same subject") {
      val subject = "presidents3"
      for {
        restClient <- ZIO.environment[ConfluentClient].map(_.confluentClient)
        _ <- restClient.registerSchema(subject, schema1)
        _ <- restClient.setConfig(subject, Forward)
        _ <- restClient.registerSchema(subject, schema2)
        versions <- restClient.subjectVersions(subject)
      } yield {
        assert(versions, equalTo(List(1, 2)))
      }
    }

    val compatibleSchemas = testM("schema1 and 2 are compatible") {
      val subject = "presidents4"
      for {
        restClient <- ZIO.environment[ConfluentClient].map(_.confluentClient)
        _ <- restClient.registerSchema(subject, schema1)
        compat <- restClient.compatible(subject, 1, schema2)
      } yield {
        assertCompletes
      }
    }

    def checkAll(testResults: Iterable[TestResult]) =
      testResults.tail.foldLeft(testResults.head)((acc, it) => acc && it)

    val modifyCompatibility = testM("modify compatibility and check") {

      val subject = "presidentsModifyCompatibility"

      def setCheck(restClient: ConfluentClientService, compat: CompatibilityLevel) =
        for {
          x <- restClient.setConfig(compat)
          _ = println(s"setConfig to $compat")
          check <- restClient.config
          _ = println(s"got back check result $check")
        } yield assert(check, equalTo(compat))

      def setCheck2(restClient: ConfluentClientService, compat: CompatibilityLevel) =
        for {
          _ <- restClient.setConfig(subject, compat)
          check <- restClient.config(subject)
        } yield assert(check, equalTo(compat))

      for {
        restClient <- ZIO.environment[ConfluentClient].map(_.confluentClient)

        general <- ZIO.collectAll(CompatibilityLevel.values.keySet.map { compat => setCheck(restClient, compat) })
        bySubject <- ZIO.collectAll(CompatibilityLevel.values.keySet.map { compat => setCheck2(restClient, compat) })
      } yield {
        checkAll(general) && checkAll(bySubject)
      }

    }



}
