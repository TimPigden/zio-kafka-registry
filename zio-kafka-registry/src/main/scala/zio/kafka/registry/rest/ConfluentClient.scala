package zio.kafka.registry.rest

import io.confluent.kafka.schemaregistry.client.rest.entities.{Schema => ConfluentSchema}
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.client.rest.{RestService => ConfluentRestService}
import org.apache.avro
import org.apache.avro.{Schema => AvroSchema}
import zio.blocking._
import zio.kafka.registry.rest.RestClient.{CompatibilityLevel, SchemaError, WrappedSchema}
import zio.kafka.registry.rest.Serializers._
import zio.{IO, RIO, Semaphore, Task}

import scala.collection.JavaConverters._

trait  ConfluentClient {
  val client: ConfluentClient.Service
}

object ConfluentClient {

  trait Service extends RestClient.Service[Blocking] {

    val rc: ConfluentRestService
    val sem: Semaphore

    private def failSchemaError[T](exception: RestClientException): RestResponse[T] =
      IO.fail(SchemaError(exception.getErrorCode, exception.getMessage))

    private def checkSchemaError[T](ex: Throwable) = ex match {
      case e: RestClientException =>
        failSchemaError[T](e)
      case x => IO.fail(ex)
    }

    def restResponse[T](f: => T): RestResponse[T] =
      sem.withPermit(
      blocking {
      try {
        IO.succeed(f)
      } catch {
        case e: Throwable => checkSchemaError(e)
      }

     }
      )

    def restResponseM[T](f: => Task[T]): RestResponse[T] =
      blocking {
      try {
        f
      } catch {
        case e: Throwable => checkSchemaError(e)
      }

       }

    override def schema(id: Int): RestResponse[avro.Schema] = restResponseM {
      parseSchemaDirect(rc.getId(id).getSchemaString)
    }

    override def subjects: RestResponse[List[String]] = restResponse {
      rc.getAllSubjects.asScala.toList
    }

    override def subjectVersions(subject: String): RestResponse[List[Int]] = restResponse {
      rc.getAllVersions(subject).asScala.toList.map(_.toInt)
    }

    override def deleteSubject(subject: String): RestResponse[List[Int]] = restResponse {
      rc.deleteSubject(Map.empty[String, String].asJava, subject).asScala.toList.map(_.toInt)
    }

    def parseWrapped(cs: ConfluentSchema): Task[WrappedSchema] = IO.effect {
      WrappedSchema(cs.getSubject, cs.getId, cs.getVersion, new AvroSchema.Parser().parse(cs.getSchema))
    }

    def wrappedSchemaResponse(f: => ConfluentSchema): RestResponse[WrappedSchema] =
      restResponseM {
        parseWrapped(f)
      }


    /**
     * @param subject
     * @param versionId if versionId = None uses latest
     */
    override def version(subject: String, versionId: Option[Int]): RestResponse[RestClient.WrappedSchema] =
      wrappedSchemaResponse(versionId.fold {
        rc.getLatestVersion(subject)
      } { version => rc.getVersion(subject, version) }
      )

    override def registerSchema(subject: String, schema: avro.Schema): RestResponse[Int] = restResponse(
      rc.registerSchema(schema.toString, subject)
    )

    override def alreadyPresent(subject: String, schema: avro.Schema): RestResponse[Option[RestClient.WrappedSchema]] =
      try {
        val cs = rc.lookUpSubjectVersion(schema.toString, subject)
        parseWrapped(cs).map(Some(_))
      } catch {
        case e: RestClientException =>
          if (e.getErrorCode == 40403)
            IO.succeed(Option.empty[WrappedSchema])
          else failSchemaError[Option[WrappedSchema]](e)
        case x => IO.fail(x)
      }

    override def delete(subject: String, versionId: Int): RestResponse[Unit] =
      restResponse(rc.deleteSchemaVersion(Map.empty[String, String].asJava, subject, versionId.toString))

    override def compatible(subject: String, versionId: Int, schema: avro.Schema): RestResponse[Boolean] =
      restResponse(rc.testCompatibility(schema.toString, subject, versionId.toString))

    override def setConfig(compatibilityLevel: RestClient.CompatibilityLevel): RestResponse[Unit] =
      restResponse(rc.updateCompatibility(CompatibilityLevel.values(compatibilityLevel), null)) // yuk!

    override def config: RestResponse[RestClient.CompatibilityLevel] =
      restResponse {
        val config = rc.getConfig(null)
        val level = config.getCompatibilityLevel
        CompatibilityLevel.values.find(_._2 == level).get._1
      }

    override def setConfig(subject: String, compatibilityLevel: RestClient.CompatibilityLevel): RestResponse[Unit] =
      restResponse(rc.updateCompatibility(CompatibilityLevel.values(compatibilityLevel), subject))

    override def config(subject: String): RestResponse[RestClient.CompatibilityLevel] =
      restResponse {
        val config = rc.getConfig(subject)
        val level = config.getCompatibilityLevel
        CompatibilityLevel.values.find(_._2 == level).get._1
      }
  }

  case class ConfluentClientImpl(confluentRestService: ConfluentRestService, semaphore: Semaphore) extends ConfluentClient {
    val client: ConfluentClient.Service = new Service {
      override val rc: ConfluentRestService = confluentRestService
      override val sem = semaphore
    }
  }
}
