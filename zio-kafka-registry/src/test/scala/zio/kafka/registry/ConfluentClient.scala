package zio.kafka.registry
import io.confluent.kafka.schemaregistry.client.rest.{RestService => ConfluentRestService}
import io.confluent.kafka.schemaregistry.client.rest.entities.{Schema => ConfluentSchema}
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro
import zio.{IO, RIO, Task}
import zio.blocking._
import zio.kafka.registry.rest.RestClient
import zio.kafka.registry.rest.RestClient.{SchemaError, WrappedSchema}
import zio.kafka.registry.rest.Serializers._

import scala.collection.JavaConverters._

case class ConfluentClient(rc: ConfluentRestService) extends RestClient.Service[Blocking] {

  private def failSchemaError[T](exception: RestClientException): RestResponse[T] =
    IO.fail(SchemaError(exception.getErrorCode, exception.getMessage))

  private def checkSchemaError[T](ex: Throwable) = ex match {
    case e: RestClientException =>
      failSchemaError[T](e)
    case x => IO.fail(ex)
  }

  def restResponse[T](f: => T): RestResponse[T] =
    blocking {
      try {
        IO.succeed(f)
      } catch {
        case e: Throwable => checkSchemaError(e)
      }
    }

  def restResponseM[T](f: => Task[T]): RestResponse[T] =
    blocking {
      try {
        f
      } catch {
        case e: Throwable => checkSchemaError(e)
      }
    }

  override def schema(id: Int): RestResponse[avro.Schema] = restResponseM {
    parseSchema(rc.getId(id).getSchemaString)
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

  def parseWrapped(cs: ConfluentSchema): Task[WrappedSchema] =
    parseSchema(cs.getSchema).map { schema =>
      WrappedSchema(cs.getSubject, cs.getId, cs.getVersion, schema)
    }

  def wrappedSchemaResponse(f: => ConfluentSchema): RestResponse[WrappedSchema] =
    restResponseM{ parseWrapped(f) }


  /**
   * @param subject
   * @param versionId if versionId = None uses latest
   */
  override def version(subject: String, versionId: Option[Int]): RestResponse[RestClient.WrappedSchema] =
    wrappedSchemaResponse( versionId.fold {
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
    restResponse(rc.updateCompatibility(compatibilityLevel.toString, null)) // yuk!

  override def config: RestResponse[RestClient.CompatibilityLevel] =
    restResponseM {
      val config = rc.getConfig(null)
      parseCompatibilityLevel(false)(config.getCompatibilityLevel)
    }

  override def setConfig(subject: String, compatibilityLevel: RestClient.CompatibilityLevel): RestResponse[Unit] =
    restResponse(rc.updateCompatibility(compatibilityLevel.toString, subject))

  override def config(subject: String): RestResponse[RestClient.CompatibilityLevel] =
    restResponseM {
      val config = rc.getConfig(subject)
      parseCompatibilityLevel(false)(config.getCompatibilityLevel)
    }
}
