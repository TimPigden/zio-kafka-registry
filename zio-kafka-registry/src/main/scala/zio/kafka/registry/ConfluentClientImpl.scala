package zio.kafka.registry

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.header.Headers
import zio.{RIO, Semaphore}
import zio.blocking.{Blocking, effectBlocking}
import zio.kafka.client.serde.Serializer
import zio.kafka.registry.rest.ConfluentClient
import zio.kafka.registry.rest.ConfluentClient.Service
import io.confluent.kafka.schemaregistry.client.rest.{RestService => ConfluentRestService}
import zio.kafka.registry.Settings.SubjectNameStrategy
import scala.collection.JavaConverters._

final case class AvroSerializer(kafkaAvroSerializer: KafkaAvroSerializer) extends Serializer[Any with Blocking, GenericRecord] {
  override def serialize(topic: String, headers: Headers, value: GenericRecord): RIO[Any with Blocking, Array[Byte]] =
    effectBlocking(kafkaAvroSerializer.serialize(topic, headers, value))
}

case class ConfluentClientImpl(confluentRestService: ConfluentRestService,
                               subjectNameStrategy: SubjectNameStrategy,
                               schemaRegistryUrl: String,
                               semaphore: Semaphore,
                               identityMapCapacity: Int = 1000,
                               additionalParams: Map[String, AnyRef] = Map.empty) extends ConfluentClient[Blocking] {
  val client = new Service[Blocking] {
    override val rc: ConfluentRestService = confluentRestService
    override val sem = semaphore
    override val avroSerializer: AvroSerializer = {
      val configMap: java.util.Map[String, _] = (additionalParams ++
        List(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY -> subjectNameStrategy.strategyClassName,
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl)
        ).asJava
      val registryClient = new CachedSchemaRegistryClient(rc,
        identityMapCapacity,
        configMap
      )
      AvroSerializer(new KafkaAvroSerializer(registryClient, configMap))
    }
  }
}
