package zio.kafka.registry
import java.util

import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider
import javax.net.ssl.SSLSocketFactory
import org.apache.avro.Schema
import zio.{RIO, Semaphore, Task, ZIO}
import zio.blocking._


/**
 * Wrapper for RestService config (security) settings. See relevant docs for
 * io.confluent.kafka.schemaregistry.client.rest.RestService (if you can find any!)
 *
 * In order to set security you should create a ConfluentClientService and then call the required method
 * on the ConfluentClientService#confluentRestService object
 */
trait ConfluentRestService {
  type RestConfigResponse[T] = RIO[Blocking, T]

  private[registry] val sem: Semaphore

  private[registry] val jrs: RestService

  /**
   * for use with ssl
   * @param sslSocketFactory
   * @return
   */
  def setSslSocketFactory(sslSocketFactory: SSLSocketFactory): RestConfigResponse[Unit] =
    sem.withPermit(
      effectBlocking(jrs.setSslSocketFactory(sslSocketFactory))
    )

  /** for use with basic auth */
  def setBasicAuthCredentialProvider(basicAuthCredentialProvider: BasicAuthCredentialProvider) : RestConfigResponse[Unit] =
    sem.withPermit(
      effectBlocking(jrs.setBasicAuthCredentialProvider(basicAuthCredentialProvider)))

  /** for use with bearer auth */
  def setBearerAuthCredentialProvider(bearerAuthCredentialProvider: BearerAuthCredentialProvider) : RestConfigResponse[Unit] =
    sem.withPermit(
      effectBlocking(jrs.setBearerAuthCredentialProvider(bearerAuthCredentialProvider)))

  /** http headers to go to schema registry server  */
  def setHttpHeaders(httpHeaders: util.Map[String, String]) : RestConfigResponse[Unit] =
    sem.withPermit(
      effectBlocking(jrs.setHttpHeaders(httpHeaders)))
}

object ConfluentRestService {
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


  def create(url: String): Task[ConfluentRestService] =
    for {
      semaphore <- Semaphore.make(1)
      rs <- ZIO.effect(new RestService(url))
    } yield new ConfluentRestService {
      override private[registry] val sem: Semaphore = semaphore
      override private[registry] val jrs = rs
    }

}