package zio.kafka.registry.rest.http4s

import org.apache.avro.Schema
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import zio.interop.catz
import zio.{Task, ZIO, ZManaged}
import zio.kafka.registry.rest.RestClient
import zio.kafka.registry.rest.RestClient.RestResponse
import zio.interop.catz._

case class Http4sRestClient() extends RestClient.Service[Any] {
  override def schema(id: Int): RestResponse[Schema] = ???

  override def subjects: RestResponse[List[String]] = ???

  override def subjectVersions(subject: String): RestResponse[List[Int]] = ???

  override def deleteSubject(string: String): RestResponse[List[Int]] = ???

  override def version(subject: String, versionId: Int): RestResponse[List[RestClient.WrappedSchema]] = ???

  override def schema(subject: String, versionId: Int): RestResponse[Schema] = ???

  override def postSchema(subject: String, schema: Schema): RestResponse[Int] = ???

  override def alreadyPresent(subject: String, schema: Schema): RestResponse[Option[RestClient.WrappedSchema]] = ???

  override def delete(subject: String, versionId: Int): RestResponse[Unit] = ???

  override def compatible(subject: String, versionId: Int, schema: Schema): RestResponse[Boolean] = ???

  override def setConfig(compatibilityLevel: RestClient.CompatibilityLevel): RestResponse[Unit] = ???

  override def config: RestResponse[RestClient.CompatibilityLevel] = ???

  override def setConfig(subject: String, compatibilityLevel: RestClient.CompatibilityLevel): RestResponse[Unit] = ???

  override def config(subject: String): RestResponse[RestClient.CompatibilityLevel] = ???
}

object Htt4sClient {
  def clientManaged: ZManaged[Any, Throwable, Client[Task]] = {
    val zioManaged = ZIO.runtime[Any].map { rts =>
      val exec = rts.Platform.executor.asEC
      implicit def rr = rts
      catz.catsIOResourceSyntax(BlazeClientBuilder[Task](exec).resource).toManaged
    }
    // for our test we need a ZManaged, but right now we've got a ZIO of a ZManaged. To deal with
    // that we create a Managed of the ZIO and then flatten it
    val mgr = zioManaged.toManaged_ // toManaged_ provides an empty release of the rescoure
    mgr.flatten
  }
}
