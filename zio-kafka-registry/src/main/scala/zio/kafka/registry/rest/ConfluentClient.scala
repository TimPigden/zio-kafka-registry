package zio.kafka.registry.rest

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.client.rest.entities.{Schema => ConfluentSchema}
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.client.rest.{RestService => ConfluentRestService}
import org.apache.avro
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema => AvroSchema}
import org.apache.kafka.common.header.Headers
import zio.blocking._
import zio.kafka.client.serde.Serializer
import zio.kafka.registry.AvroSerializer
import zio.kafka.registry.rest.RestClient.{CompatibilityLevel, SchemaError, WrappedSchema}
import zio.kafka.registry.rest.Serializers._
import zio.{IO, RIO, Semaphore, Task}

import scala.collection.JavaConverters._

trait ConfluentClient[R <: Blocking] {
  val client: ConfluentClient.Service[R]
}

object ConfluentClient {

  trait Service[R <: Blocking]  {
    type RestResponse[T] = RIO[R, T]

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
            IO.effectTotal(f)
          } catch {
            case e: Throwable => checkSchemaError(e)
          }

        }
      )

    def restResponseM[T](f: => Task[T]): RestResponse[T] =
      sem.withPermit(
        blocking {
          try {
            f
          } catch {
            case e: Throwable => checkSchemaError(e)
          }

        }
      )

    def schema(id: Int): RestResponse[avro.Schema] = restResponseM {
      parseSchemaDirect(rc.getId(id).getSchemaString)
    }

    def subjects: RestResponse[List[String]] = restResponse {
      rc.getAllSubjects.asScala.toList
    }

    def subjectVersions(subject: String): RestResponse[List[Int]] = restResponse {
      rc.getAllVersions(subject).asScala.toList.map(_.toInt)
    }

    def deleteSubject(subject: String): RestResponse[List[Int]] = restResponse {
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
    def version(subject: String, versionId: Option[Int]): RestResponse[RestClient.WrappedSchema] =
      wrappedSchemaResponse(versionId.fold {
        rc.getLatestVersion(subject)
      } { version => rc.getVersion(subject, version) }
      )

    def registerSchema(subject: String, schema: avro.Schema): RestResponse[Int] = restResponse(
      rc.registerSchema(schema.toString, subject)
    )

    def alreadyPresent(subject: String, schema: avro.Schema): RestResponse[Option[RestClient.WrappedSchema]] =
      try {
        val cs = rc.lookUpSubjectVersion(schema.toString, subject)
        parseWrapped(cs).map(Some(_))
      } catch {
        case e: RestClientException =>
          if (e.getErrorCode == 40403)
            IO.effectTotal(Option.empty[WrappedSchema])
          else failSchemaError[Option[WrappedSchema]](e)
        case x => IO.fail(x)
      }

    def delete(subject: String, versionId: Int): RestResponse[Unit] =
      restResponse(rc.deleteSchemaVersion(Map.empty[String, String].asJava, subject, versionId.toString))

    def compatible(subject: String, versionId: Int, schema: avro.Schema): RestResponse[Boolean] =
      restResponse(rc.testCompatibility(schema.toString, subject, versionId.toString))

    def setConfig(compatibilityLevel: RestClient.CompatibilityLevel): RestResponse[Unit] =
      restResponse(rc.updateCompatibility(CompatibilityLevel.values(compatibilityLevel), null)) // yuk!

    def config: RestResponse[RestClient.CompatibilityLevel] =
      restResponse {
        val config = rc.getConfig(null)
        val level = config.getCompatibilityLevel
        CompatibilityLevel.values.find(_._2 == level).get._1
      }

    def setConfig(subject: String, compatibilityLevel: RestClient.CompatibilityLevel): RestResponse[Unit] =
      restResponse(rc.updateCompatibility(CompatibilityLevel.values(compatibilityLevel), subject))

    def config(subject: String): RestResponse[RestClient.CompatibilityLevel] =
      restResponse {
        val config = rc.getConfig(subject)
        val level = config.getCompatibilityLevel
        CompatibilityLevel.values.find(_._2 == level).get._1
      }

    val avroSerializer: AvroSerializer
  }

}
