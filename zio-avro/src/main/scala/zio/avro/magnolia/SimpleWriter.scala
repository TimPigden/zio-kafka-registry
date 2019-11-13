package zio.avro.magnolia

import java.nio.{ByteBuffer, FloatBuffer}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId, ZonedDateTime}

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import zio.avro.magnolia.SimpleSchemaGenerator.ZoneInstant

import scala.collection.JavaConverters._

trait SimpleWriter  {
  implicit def intAvroWriter[T <: Int]: AvroWriter[T] =  { (_, value) => value: Integer   }

  val simpleIntAvroWriter = intAvroWriter[Int]

  implicit def longAvroWriter[T <: Long]: AvroWriter[T] =  { (_, value) => value : java.lang.Long }

  val simpleLongAvroWriter = longAvroWriter[Long]

  implicit def doubleAvroWriter[T <: Double]: AvroWriter[T] = { (_, value) => value : java.lang.Double  }

  val simpleDoubleAvroWriter = doubleAvroWriter[Double]

  implicit def floatAvroWriter[T <: Float]: AvroWriter[T] = { (_, value) => value:  java.lang.Float  }

  implicit def stringAvroWriter[T <: String]: AvroWriter[T] = { (_, value) => value  }

  val simpleStringAvroWriter = stringAvroWriter[String]

  implicit def booleanAvroWriter: AvroWriter[Boolean] = { (_, value) => value : java.lang.Boolean  }

  implicit def floatArrayAvroWriter: AvroWriter[Array[Float]] = { (_, value) =>
    val bb = ByteBuffer.allocate(value.length * 4)
    value.indices.foreach { i => bb.putFloat(value(i))}
    bb.rewind()
  }
  implicit def longArrayAvroWriter: AvroWriter[Array[Long]] = { (_, value) =>
    val bb = ByteBuffer.allocate(value.length * 8)
    value.indices.foreach { i => bb.putLong(value(i))}
    bb.rewind()
  }
  implicit def intArrayAvroWriter: AvroWriter[Array[Int]] = { (_, value) =>
    val bb = ByteBuffer.allocate(value.length * 4)
    value.indices.foreach { i => bb.putInt(value(i))}
    bb.rewind()
  }
  implicit def doubleArrayAvroWriter: AvroWriter[Array[Double]] = { (_, value) =>
    val bb = ByteBuffer.allocate(value.length * 8)
    value.indices.foreach { i => bb.putDouble(value(i))}
    bb.rewind()
  }
  implicit def byteArrayAvroWriter: AvroWriter[Array[Byte]] = { (_, value) =>
    val bb = ByteBuffer.wrap(value)
    bb.rewind()
  }

  def iterableAvroWriter[T](implicit tWriter: AvroWriter[T]): AvroWriter[Iterable[T]] = { (schema, value) =>
    val elType = schema.getElementType
    val children = value.toList.map { v => tWriter.write(elType, v)}
    new GenericData.Array(schema, children.asJava)
  }

  implicit def listAvroWriter[T](implicit tWriter: AvroWriter[T]): AvroWriter[List[T]] = new AvroWriter[List[T]] {
    private val iterable = iterableAvroWriter[T]
    override def write(schema: Schema, value: List[T]): AnyRef = iterable.write(schema, value)
  }

  implicit def setAvroWriter[T](implicit tWriter: AvroWriter[T]): AvroWriter[Set[T]] = new AvroWriter[Set[T]] {
    private val iterable = iterableAvroWriter[T]
    override def write(schema: Schema, value: Set[T]): AnyRef = iterable.write(schema, value)
  }

  def stringMapAvroWriter[K <: String, T](implicit tWriter: AvroWriter[T]): AvroWriter[Map[K, T]] = { (schema, value) =>
    val elType = schema.getValueType
    val children: java.util.Map[String, AnyRef] = value.map { p =>
      val s: String = p._1
      val v = p._2
      s -> tWriter.write(elType, v)
    }.asJava
    children
  }

  def kvMapAvroWriter[K, T](implicit kWriter: AvroWriter[K], tWriter: AvroWriter[T]): AvroWriter[Map[K, T]] = new AvroWriter[Map[K, T]] {
    private val tupWriter = AvroWriterDerivation.avroWriter[(K, T)]
    private val itWriter = iterableAvroWriter(tupWriter)

    override def write(schema: Schema, value: Map[K, T]): AnyRef = itWriter.write(schema, value.toList)
  }

  implicit def optionWriter[T](implicit tWriter: AvroWriter[T]): AvroWriter[Option[T]] = { (schema, value) =>
    value match {
      case Some(t) => tWriter.write(schema, t)
      case None => null
    }
  }

  implicit val instantWriter: AvroWriter[Instant] = { (schema, value) =>
    simpleLongAvroWriter.write(schema, value.toEpochMilli)
  }

  implicit val localDateWriter: AvroWriter[LocalDate] = { (schema, value) =>
    simpleLongAvroWriter.write(schema, value.toEpochDay)
  }
  implicit val localDateTimeWriter: AvroWriter[LocalDateTime] = { (schema, value) =>
    val inst = value.atZone(utcId).toInstant
    simpleLongAvroWriter.write(schema, inst.toEpochMilli)
  }

  implicit val localTimeWriter: AvroWriter[LocalTime] ={ (schema, value) =>
    val asMillis = value.toNanoOfDay / 1000000L
    longAvroWriter.write(schema, asMillis)
  }

  implicit val ZoneIdWriter: AvroWriter[ZoneId] = { (schema, value) =>
    simpleStringAvroWriter.write(schema, value.getId)
  }

  private val zoneInstantWriter = AvroWriterDerivation.avroWriter[ZoneInstant]

  implicit val zonedDateTimeWriter: AvroWriter[ZonedDateTime] = { (schema, value) =>
    val zii = ZoneInstant(value.getZone.getId, value.toInstant)
    zoneInstantWriter.write(schema, zii)
  }


}
object SimpleWriter extends SimpleWriter
