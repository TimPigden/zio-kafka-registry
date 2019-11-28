package zio.kafka.registry

import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.header.Headers
import zio.RIO
import zio.blocking._
import zio.kafka.client.serde.{Deserializer, Serializer}

import scala.reflect.ClassTag
import com.sksamuel.avro4s.RecordFormat

object AvroSerdes {

  final case class AvroSerializer(kafkaAvroSerializer: KafkaAvroSerializer) extends Serializer[Any with Blocking, GenericRecord] {
    override def serialize(topic: String, headers: Headers, value: GenericRecord): RIO[Any with Blocking, Array[Byte]] =
      effectBlocking(kafkaAvroSerializer.serialize(topic, headers, value))
  }

  final case class AvroDeserializer[T : ClassTag](kafkaAvroDeserializer: KafkaAvroDeserializer) extends Deserializer[Any with Blocking, T] {
    override def deserialize(topic: String, headers: Headers, data: Array[Byte]): RIO[Any with Blocking, T] =
      effectBlocking {
        val obj = kafkaAvroDeserializer.deserialize(topic, headers, data)
        obj match {
          case t: T => t
        }
      }
  }

  final case class AvroGenericDeserializer[T](kafkaAvroDeserializer: KafkaAvroDeserializer,
                                              recordFormat: RecordFormat[T]) extends Deserializer[Any with Blocking, T] {
    override def deserialize(topic: String, headers: Headers, data: Array[Byte]): RIO[Any with Blocking, T] =
      effectBlocking {
        val obj = kafkaAvroDeserializer.deserialize(topic, headers, data)
        obj match {
          case t: GenericRecord =>
            recordFormat.from(t)
        }
      }
  }
}
