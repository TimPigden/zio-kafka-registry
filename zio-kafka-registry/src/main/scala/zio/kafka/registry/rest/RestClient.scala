package zio.kafka.registry.rest

import org.apache.avro.Schema
import zio._
import zio.blocking.Blocking
import zio.kafka.registry.rest.RestClient._

trait AbstractClient {
  val client: AbstractClient.Service
}

object AbstractClient {
  trait Service
}

trait RestClient[R] {
  val restClient: RestClient.Service[R]
}

object RestClient {
  case class SchemaError(errorCode: Int, message: String) extends Throwable

  case class WrappedSchema(subject: String, id: Int, version: Int, schema: Schema)


  sealed trait CompatibilityLevel
  case object Backward extends CompatibilityLevel
  case object BackwardTransitive extends CompatibilityLevel
  case object Forward extends CompatibilityLevel
  case object ForwardTransitive extends CompatibilityLevel
  case object Full extends CompatibilityLevel
  case object FullTransitive extends CompatibilityLevel
  case object NoCompatibilityLevel extends CompatibilityLevel // didn't want to muddy the waters with "None"

  object CompatibilityLevel {
    val values: Map[CompatibilityLevel, String] = Map(
      Backward  -> "BACKWARD",
      BackwardTransitive  -> "BACKWARD_TRANSITIVE",
      Forward  -> "FORWARD",
      ForwardTransitive  -> "FORWARD_TRANSITIVE",
      Full  -> "FULL",
      FullTransitive  -> "FULL_TRANSITIVE",
      NoCompatibilityLevel  -> "NONE",
    )
  }

  trait Service[R] extends AbstractClient.Service {
    type RestResponse[T] = RIO[R, T]

    def schema(id: Int): RestResponse[Schema]

    def subjects: RestResponse[List[String]]

    def subjectVersions(subject: String): RestResponse[List[Int]]

    def deleteSubject(subject: String): RestResponse[List[Int]]

    /**
     * @param subject
     * @param versionId if versionId = None uses latest
     */
    def version(subject: String, versionId: Option[Int]): RestResponse[WrappedSchema]

    def registerSchema(subject: String, schema: Schema): RestResponse[Int]

    def alreadyPresent(subject: String, schema: Schema): RestResponse[Option[WrappedSchema]]

    def delete(subject: String, versionId: Int): RestResponse[Unit]

    def compatible(subject: String, versionId: Int, schema: Schema): RestResponse[Boolean]

    def setConfig(compatibilityLevel: CompatibilityLevel): RestResponse[Unit]

    def config: RestResponse[CompatibilityLevel]

    def setConfig(subject: String, compatibilityLevel: CompatibilityLevel): RestResponse[Unit]

    def config(subject: String): RestResponse[CompatibilityLevel]
  }

}
