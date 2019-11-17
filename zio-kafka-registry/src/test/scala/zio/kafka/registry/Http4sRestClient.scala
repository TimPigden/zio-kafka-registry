package zio.kafka.registry

import org.apache.zookeeper.proto.ErrorResponse
import org.http4s.Uri.{Authority, Scheme}
import org.http4s._
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.headers.`Content-Type`
import zio.interop.catz
import zio.interop.catz._
import zio.kafka.registry.rest.{AbstractClient, Serializers}
import zio.kafka.registry.rest.RestClient.{RestResponse, SchemaError}
import zio.{IO, Task, ZIO, ZManaged}
import Kafka._
import Http4sClient.uri

object Http4sClient {

  def uri(path: String) = {
    val res = Uri.unsafeFromString(s"http://localhost:$schemaRegistryPort$path")
    println(s"res is $res")
    res
  }

  def make: ZManaged[Any, Throwable, Client[Task]] = {
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

case class Http4sClient(client: Client[Task]) extends AbstractClient {

  def errBody(errResponse: Response[Task]): Task[SchemaError] =
    for {
      body <- errResponse.as[String]
      sErr <- Serializers.parseSchemaError(body)
    } yield sErr

  def requestOut[T](reqIn: Request[Task])(implicit tParser: String => Task[T]): RestResponse[T] = {
    val req = reqIn.withHeaders(reqIn.headers.put(Header("Accept", "application/vnd.schemaregistry.v1+json" )) )
    val errf: Response[Task] => Task[Throwable] = errBody
    for {
      asString <- client.expectOr[String](req){ errf }
      t <- tParser(asString)
    } yield t
  }

  def media = mediaType"application/vnd.schemaregistry.v1+json"

  override def get[T](url: String)(implicit tParser: String => Task[T]): RestResponse[T] =
    requestOut(Request[Task](Method.GET, uri(url)))

  def inOut[In, Out](method: Method, url: String, in: In)(implicit inWriter: In => String, outParser: String => Task[Out]): RestResponse[Out] = {
    val asString = inWriter(in)
    println(s"for $method :  $url : $asString")
    val req = Request[Task](method, uri(url))
      .withEntity[String](asString)
      .withContentType(`Content-Type`(media))
    val errf: Response[Task] => Task[Throwable] = errBody
    requestOut[Out](req)
  }

  override def post[In, Out](url: String, in: In)(implicit inWriter: In => String, outParser: String => Task[Out]): RestResponse[Out] =
    inOut[In, Out](Method.POST, url, in)

  override def put[In, Out](url: String, in: In)(implicit inWriter: In => String, outParser: String => Task[Out]): RestResponse[Out] =
    inOut[In, Out](Method.PUT, url, in)

  override def delete[Out](url: String)(implicit outParser: String => Task[Out]): RestResponse[Out] =
    requestOut(Request[Task](Method.DELETE, uri(url)))

}
