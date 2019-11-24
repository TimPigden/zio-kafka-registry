package zio.kafka.registry.rest

import org.apache.avro.Schema
import zio._
import zio.blocking.Blocking
import zio.kafka.registry.rest.RestClient._

trait RestClient {
  val restClient: RestClient.Service[Any]
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

  trait Service[R] {
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

case class RestClientImpl(abstractClient: AbstractClient[Any]) extends RestClient.Service[Any] {
  import Serializers._

  override def schema(id: Int): RestResponse[Schema] =
    abstractClient.get(Urls.schema(id))

  override def subjects: RestResponse[List[String]] =
    abstractClient.get(Urls.subjects)

  override def subjectVersions(subject: String): RestResponse[List[Int]] =
    abstractClient.get(Urls.subjectVersions(subject))

  override def deleteSubject(subject: String): RestResponse[List[Int]] =
    abstractClient.delete(Urls.deleteSubject(subject))

  /**
   * @param subject
   * @param versionId if versionId = None uses latest
   */
  override def version(subject: String, versionId: Option[Int]): RestResponse[RestClient.WrappedSchema] =
    abstractClient.get(Urls.version(subject, versionId))

  override def registerSchema(subject: String, schema: Schema): RestResponse[Int] =
    abstractClient.post(Urls.postSchema(subject), schema)

  override def alreadyPresent(subject: String, schema: Schema): RestResponse[Option[RestClient.WrappedSchema]] = {
    val opt = abstractClient.post[Schema, RestClient.WrappedSchema](Urls.alreadyPresent(subject), schema)
    for {
      res <- opt.either.map {
        case Left(err) => err match {
          case SchemaError(errorCode, message) =>
            if (errorCode == 40403) IO.succeed(Option.empty[RestClient.WrappedSchema])
            else IO.fail(err)
          case _ => IO.fail(err)
        }
        case Right(ok) => IO.succeed(Some(ok))
      }
      f <- res
    } yield f
  }

  override def delete(subject: String, versionId: Int): RestResponse[Unit] =
    abstractClient.delete[String](Urls.delete(subject, versionId)).map(_ => ())

  override def compatible(subject: String, versionId: Int, schema: Schema): RestResponse[Boolean] =
    abstractClient.post(Urls.compatible(subject, versionId), schema)

  override def setConfig(compatibilityLevel: CompatibilityLevel): RestResponse[Unit] = {
    implicit val pc = parseCompatibilityLevel(true) _
    abstractClient.put[CompatibilityLevel, CompatibilityLevel](Urls.setConfig, compatibilityLevel).map(_ => ())
  }

  override def config: RestResponse[CompatibilityLevel] = {
    implicit val pc = parseCompatibilityLevel(false) _
    abstractClient.get(Urls.config)
  }

  override def setConfig(subject: String, compatibilityLevel: CompatibilityLevel): RestResponse[Unit] = {
    implicit val pc = parseCompatibilityLevel(true) _
    abstractClient.put[CompatibilityLevel, CompatibilityLevel](Urls.setConfig(subject), compatibilityLevel)
      .map(_ => ())
  }

  override def config(subject: String): RestResponse[CompatibilityLevel] = {
    implicit val pc = parseCompatibilityLevel(false) _
    abstractClient.get(Urls.config(subject))
  }
}
