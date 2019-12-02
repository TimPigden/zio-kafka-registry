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
import zio.kafka.registry.Settings.SubjectNameStrategy
import zio.{IO, RIO, Semaphore, Task, ZManaged}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Service to interface with schema
 */
trait ConfluentClientService {

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

  /**
   * gets schema for given id as avro.Schema
   * @param id
   * @return schema requested. Error if missing
   */
  def schema(id: Int): RestResponse[avro.Schema] = restResponseM {
    parseSchemaDirect(jrs.getId(id).getSchemaString)
  }

  /**
    * @return list of schema subject names
   */
  def subjects: RestResponse[List[String]] = restResponse {
    jrs.getAllSubjects.asScala.toList
  }

  /**
   * gets all version ids for given subject
   * @param subject
   * @return list of version numbers for given subject
   */
  def subjectVersions(subject: String): RestResponse[List[Int]] = restResponse {
    jrs.getAllVersions(subject).asScala.toList.map(_.toInt)
  }

  /**
   * delets subject with all versions. Confluent docs recommend only using this in testing!
   * @param subject
   * @return List of deleted version numbers
   */
  def deleteSubject(subject: String): RestResponse[List[Int]] = restResponse {
    jrs.deleteSubject(Map.empty[String, String].asJava, subject).asScala.toList.map(_.toInt)
  }

  private def parseWrapped(cs: ConfluentSchema): Task[WrappedSchema] = IO.effect {
    WrappedSchema(cs.getSubject, cs.getId, cs.getVersion, new AvroSchema.Parser().parse(cs.getSchema))
  }

  private def wrappedSchemaResponse(f: => ConfluentSchema): RestResponse[WrappedSchema] =
    restResponseM {
      parseWrapped(f)
    }


  /**
   * gets a specific schema
   * @param subject subject name
   * @param versionId if versionId = None uses latest
   * @return with metadata (as WrappedSchema). Error if version Id does not exist or if versionId is None, there is
   *         no latest
   */
  def version(subject: String, versionId: Option[Int]): RestResponse[WrappedSchema] =
    wrappedSchemaResponse(versionId.fold {
      jrs.getLatestVersion(subject)
    } { version => jrs.getVersion(subject, version) }
    )

  /**
   * registers a schema with given subject, returning unique schema id in registry
   * @param subject
   * @param schema
   * @return unique schema id
   */
  def registerSchema(subject: String, schema: avro.Schema): RestResponse[Int] = restResponse(
    jrs.registerSchema(schema.toString, subject)
  )

  /**
   * checks if schema already defined for given subject. If so returns schema
   * @param subject
   * @param schema
   * @return schema with metadata as WrappedSchema
   */
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

  /**
   * deletes a specific version of a schema
   * @param subject
   * @param versionId
   * @return Unit - error if does not exist
   */
  def delete(subject: String, versionId: Int): RestResponse[Unit] =
    restResponse{jrs.deleteSchemaVersion(Map.empty[String, String].asJava, subject, versionId.toString)
      ()}

  /**
   * Checks compatibility of proposed schema with the schema identified by subject / versionId according to the current
   * compatibilityLevel
   * @param subject
   * @param versionId
   * @param schema
   * @return true if compatible else false
   */
  def compatible(subject: String, versionId: Int, schema: avro.Schema): RestResponse[Boolean] =
    restResponse(jrs.testCompatibility(schema.toString, subject, versionId.toString))

  /**
   * Sets compatibility level for registry server as a whole
   * @param compatibilityLevel new compatibility level
   * @return Unit
   */
  def setConfig(compatibilityLevel: CompatibilityLevel): RestResponse[Unit] =
    restResponse{
      jrs.updateCompatibility(CompatibilityLevel.values(compatibilityLevel), null)
      ()
    } // yuk!

  /**
   * gets current compatility level for registry server as a whole
   * @return CompatibilityLevel
   */
  def config: RestResponse[CompatibilityLevel] =
    restResponse {
      val config = jrs.getConfig(null)
      val level = config.getCompatibilityLevel
      CompatibilityLevel.values.find(_._2 == level).get._1
    }

  /**
   * sets the compatibilityLevel for a speciric subject
   * @param subject subject name
   * @param compatibilityLevel desired level
   * @return Unit
   */
  def setConfig(subject: String, compatibilityLevel: CompatibilityLevel): RestResponse[Unit] =
    restResponse {
      jrs.updateCompatibility(CompatibilityLevel.values(compatibilityLevel), subject)
      ()
    }

  /**
   * gets compatibilityLevel for given subject
   * @param subject
   * @return the level for the subject. (Confluent docs do not define behaviour if none set for this subject but presumably
   *         defaults to server compatibility level)
   */
  def config(subject: String): RestResponse[CompatibilityLevel] =
    restResponse {
      val config = jrs.getConfig(subject)
      val level = config.getCompatibilityLevel
      CompatibilityLevel.values.find(_._2 == level).get._1
    }

  /**
   * creates a new serializer to be used with generic record. Will perform registry lookup and use ids in
   * output data rather than embed whole schema
   * @param subjectNameStrategy a SubjectNameStrategy
   * @param additionalParams any additional parameters as required.  You do not need to supply registry url
   * @return the AvroSerializer object
   */
  def avroSerializer(subjectNameStrategy: SubjectNameStrategy,
                     additionalParams: Map[String, Any] = Map.empty): RestResponse[AvroSerializer] =
    effectBlocking {
      val params = (additionalParams ++
        List(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY -> subjectNameStrategy.strategyClassName,
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url)).asJava
      AvroSerializer(new KafkaAvroSerializer(registryClient, params))
    }

  /**
   * creates a deserializer that will work with given T (note it will fail if the data is not of the correct type.
   * And it only supports those types supported by registry
   * @param subjectNameStrategy
   * @param additionalParams any additional parameters as required. You do not need to supply registry url
   * @tparam T
   * @return
   */
  def avroDeserializer[T : ClassTag](subjectNameStrategy: SubjectNameStrategy,
                       additionalParams: Map[String, Any] = Map.empty): RestResponse[AvroDeserializer[T]] =
    effectBlocking {
      val params = (additionalParams ++
        List(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY -> subjectNameStrategy.strategyClassName,
          AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url)).asJava
      AvroDeserializer[T](new KafkaAvroDeserializer(registryClient, params))
    }

  /**
   * creates a deserializer that expects a generic record and converts to type T using the supplied avro4s recordFormat
   * @param subjectNameStrategy
   * @param additionalParams any additional parameters as required. You do not need to supply registry url
   * @param recordFormat
   * @tparam T the target type
   * @return a serializer for the specific type
   */
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

  /**
   * creates a ConfluentClientService
   * @param urlIn the single url used to find the registry
   * @param identityMapCapacity number if distinct schemas to cache
   * @return the service
   */

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

/**
 * Module pattern for ConfluentClientService
 */
trait ConfluentClient {
  val confluentClient: ConfluentClientService
}

object ConfluentClient {

  /**
   * creates a ConfluentClient as ZManaged
   * @param url the single url used to find the registry
   * @param identityMapCapacity number if distinct schemas to cache
   * @return the service
   */

  def make[R <: Blocking](url: String,
                          identityMapCapacity: Int): ZManaged[R, Throwable, ConfluentClient] =
    ZManaged.fromEffect(ConfluentClientService.create(url, identityMapCapacity).map { z => new ConfluentClient {
      override val confluentClient: ConfluentClientService = z
    }}
  )

}

