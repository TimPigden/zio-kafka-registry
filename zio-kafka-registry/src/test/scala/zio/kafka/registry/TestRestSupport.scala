package zio.kafka.registry

import zio.{URIO, ZIO}
import zio.kafka.registry.rest.{AbstractClient, ConfluentClient, RestClient}
import zio.test._
import Assertion._
import TestRestSupport._
import KafkaTestUtils._
import com.sksamuel
import com.sksamuel.avro4s.AvroSchema
import zio.blocking.Blocking
import zio.clock.Clock.Live
import zio.duration.Duration
import zio.kafka.registry.rest.RestClient.{Backward, BackwardTransitive, CompatibilityLevel, Forward, ForwardTransitive, Full, FullTransitive, NoCompatibilityLevel}

object TestRestSupport {
  case class President1(name: String)
  case class President2(name: String, age: Int)


  val schema1 = AvroSchema[President1]
  val schema2 = AvroSchema[President2]


  val confluentAllTests = new AllTests[Blocking, ConfluentClient, ConfluentClient.Service] {
    override def restClientService =
      ZIO.environment[ConfluentClient].map {_.client}
  }

  trait AllTests[R, RC, RCS <: RestClient.Service[R]] {
    def restClientService: URIO[RC, RCS]

    def allTests = List(
//      testSubjects,
//      testDelete,
      modifyCompatibility,
//      checkDeleteSubject,
//      multipleSchemas,
//      compatibleSchemas
    )

    val testSubjects
    = testM("test subjects empty then non-empty") {
      val subject = "presidents"
      for {
        restClient <- restClientService
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
        restClient <- restClientService
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
        restClient <- restClientService
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
        restClient <- restClientService
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
        restClient <- restClientService
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

      def setCheck[R](restClient: RestClient.Service[R], compat: CompatibilityLevel) =
        for {
          x <- restClient.setConfig(compat)
          _ = println(s"setConfig to $compat")
          check <- restClient.config
          _ = println(s"got back check result $check")
        } yield assert(check, equalTo(compat))

      def setCheck2[R](restClient: RestClient.Service[R], compat: CompatibilityLevel) =
        for {
          _ <- restClient.setConfig(subject, compat)
          check <- restClient.config(subject)
        } yield assert(check, equalTo(compat))

      for {
        restClient <- restClientService
        g1 <- setCheck(restClient, Backward)
        g2 <- setCheck(restClient, BackwardTransitive)
        g3 <- setCheck(restClient, Forward)
        g4 <- setCheck(restClient, ForwardTransitive)
        g5 <- setCheck(restClient, Full)
        g6 <- setCheck(restClient, FullTransitive)
        g7 <- setCheck(restClient, NoCompatibilityLevel)
//        general <- ZIO.collectAll(CompatibilityLevel.values.keySet.map { compat => setCheck(restClient, compat) })
//        bySubject <- ZIO.collectAll(CompatibilityLevel.values.keySet.map { compat => setCheck2(restClient, compat) })
      } yield {
        checkAll(List(g1, g2, g3, g4, g5, g6, g7))/* &&
          checkAll(bySubject)*/
      }

    }
  }


}
