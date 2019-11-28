package zio.kafka.registry

import io.confluent.kafka.schemaregistry.client.rest.entities.{Schema => ConfluentSchema}
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro
import org.apache.avro.{Schema => AvroSchema}
import zio.blocking._
import zio.kafka.registry.ConfluentRestService.{CompatibilityLevel, SchemaError, WrappedSchema}
import Serializers._
import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import zio.kafka.registry.AvroSerdes.{AvroDeserializer, AvroGenericDeserializer, AvroSerializer}
import zio.kafka.registry.ConfluentClientService._
import zio.kafka.registry.Settings.SubjectNameStrategy
import zio.{IO, Managed, RIO, Semaphore, Task, ZIO, ZManaged}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

trait ConfluentClientService { //} extends Service[Blocking] {

  type RestResponse[T] = RIO[Blocking, T]

  val url: String
  val confluentRestService: ConfluentRestService
  protected val sem: Semaphore
  protected[registry] val registryClient: CachedSchemaRegistryClient

  private def jrs = confluentRestService.jrs

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
    parseSchemaDirect(jrs.getId(id).getSchemaString)
  }

  def subjects: RestResponse[List[String]] = restResponse {
    jrs.getAllSubjects.asScala.toList
  }

  def subjectVersions(subject: String): RestResponse[List[Int]] = restResponse {
    jrs.getAllVersions(subject).asScala.toList.map(_.toInt)
  }

  def deleteSubject(subject: String): RestResponse[List[Int]] = restResponse {
    jrs.deleteSubject(Map.empty[String, String].asJava, subject).asScala.toList.map(_.toInt)
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
  def version(subject: String, versionId: Option[Int]): RestResponse[WrappedSchema] =
    wrappedSchemaResponse(versionId.fold {
      jrs.getLatestVersion(subject)
    } { version => jrs.getVersion(subject, version) }
    )

  def registerSchema(subject: String, schema: avro.Schema): RestResponse[Int] = restResponse(
    jrs.registerSchema(schema.toString, subject)
  )

  def alreadyPresent(subject: String, schema: avro.Schema): RestResponse[Option[WrappedSchema]] =
    try {
      val cs = jrs.lookUpSubjectVersion(schema.toString, subject)
      parseWrapped(cs).map(Some(_))
    } catch {
      case e: RestClientException =>
        if (e.getErrorCode == 40403)
          IO.effectTotal(Option.empty[WrappedSchema])
        else failSchemaError[Option[WrappedSchema]](e)
      case x: Throwable => IO.fail(x)
    }

  def delete(subject: String, versionId: Int): RestResponse[Unit] =
    restResponse{jrs.deleteSchemaVersion(Map.empty[String, String].asJava, subject, versionId.toString)
      ()}

  def compatible(subject: String, versionId: Int, schema: avro.Schema): RestResponse[Boolean] =
    restResponse(jrs.testCompatibility(schema.toString, subject, versionId.toString))

  def setConfig(compatibilityLevel: CompatibilityLevel): RestResponse[Unit] =
    restResponse{
      jrs.updateCompatibility(CompatibilityLevel.values(compatibilityLevel), null)
      ()
    } // yuk!

  def config: RestResponse[CompatibilityLevel] =
    restResponse {
      val config = jrs.getConfig(null)
      val level = config.getCompatibilityLevel
      CompatibilityLevel.values.find(_._2 == level).get._1
    }

  def setConfig(subject: String, compatibilityLevel: CompatibilityLevel): RestResponse[Unit] =
    restResponse {
      jrs.updateCompatibility(CompatibilityLevel.values(compatibilityLevel), subject)
      ()
    }

  def config(subject: String): RestResponse[CompatibilityLevel] =
    restResponse {
      val config = jrs.getConfig(subject)
      val level = config.getCompatibilityLevel
      CompatibilityLevel.values.find(_._2 == level).get._1
    }

  def avroSerializer(subjectNameStrategy: SubjectNameStrategy,
                     additionalParams: Map[String, Any] = Map.empty): RestResponse[AvroSerializer] =
    effectBlocking {
      val params = (additionalParams ++
        List(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY -> subjectNameStrategy.strategyClassName,
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url)).asJava
      AvroSerializer(new KafkaAvroSerializer(registryClient, params))
    }

  def avroDeserializer[T : ClassTag](subjectNameStrategy: SubjectNameStrategy,
                       additionalParams: Map[String, Any] = Map.empty): RestResponse[AvroDeserializer[T]] =
    effectBlocking {
      val params = (additionalParams ++
        List(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY -> subjectNameStrategy.strategyClassName,
          AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url)).asJava
      AvroDeserializer[T](new KafkaAvroDeserializer(registryClient, params))
    }

  def avroGenericDeserializer[T](subjectNameStrategy: SubjectNameStrategy,
                                 additionalParams: Map[String, Any] = Map.empty)
                                (recordFormat: RecordFormat[T]): RestResponse[AvroGenericDeserializer[T]] =
    effectBlocking {
      val params = (additionalParams ++
        List(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY -> subjectNameStrategy.strategyClassName,
          AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url)).asJava
      AvroGenericDeserializer[T](new KafkaAvroDeserializer(registryClient, params), recordFormat)
    }

}

object ConfluentClientService {

  def create(urlIn: String,
             identityMapCapacity: Int): Task[ConfluentClientService] =
    for {
      rs <- ConfluentRestService.create(urlIn)
      cc <- IO.effect(new ConfluentClientService {
        override val url: String = urlIn
        override val confluentRestService: ConfluentRestService = rs
        override val registryClient = new CachedSchemaRegistryClient(confluentRestService.jrs, identityMapCapacity)
        override val sem = confluentRestService.sem
      })
    } yield cc

}

trait ConfluentClient {
  def confluentClient: ConfluentClientService

}
object ConfluentClient {

  def make[R <: Blocking](url: String,
                          identityMapCapacity: Int): ZManaged[R, Throwable, ConfluentClient] =
    ZManaged.fromEffect(ConfluentClientService.create(url, identityMapCapacity).map { z => new ConfluentClient {
      override def confluentClient: ConfluentClientService = z
    }}
  )

}

