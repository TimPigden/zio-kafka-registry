package zio.avro

import java.io.File

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import zio._
import zio.avro.magnolia.{AvroCompiler, AvroReader, AvroWriter, SchemaGenerator}
import zio.blocking.{blocking, effectBlocking}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object AvroSerialize  {

  def writeBinary[T](ts: Iterable[T])
                    (dfCreate: (DataFileWriter[GenericRecord], Schema) => Unit)
                    (implicit schemaGenerator: SchemaGenerator[T], avroWriter: AvroWriter[T]): RIO[ZEnv, Unit] =
    effectBlocking {
      val asJson = schemaGenerator.generate
      val schema = AvroCompiler.compile(asJson)

      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter(datumWriter)
      dfCreate(dataFileWriter, schema)
      ts.foreach { t =>
        val gr = avroWriter.write(schema, t).asInstanceOf[GenericRecord]
        dataFileWriter.append(gr.asInstanceOf[GenericRecord])
      }
      dataFileWriter.flush()
      dataFileWriter.close()
    }

  def writeBinaryFile[T](ts: Iterable[T], file: File)
                        (implicit schemaGenerator: SchemaGenerator[T], avroWriter: AvroWriter[T]): RIO[ZEnv, Unit] =
    writeBinary(ts){ (dfw, schema) => dfw.create(schema, file)
      ()
    }

  def readBinaryFile[T](file: File)(implicit schemaGenerator: SchemaGenerator[T], avroReader: AvroReader[T]): RIO[ZEnv, List[T]] =
    blocking {
      val schema = AvroCompiler.compile(schemaGenerator.generate)
      val datumReader = new GenericDatumReader[AnyRef](schema)
      IO.succeed(new DataFileReader(file, datumReader)).bracket { df => IO.succeed(df.close()) } { dataFileReader =>
        val record = new GenericData.Record(schema)
        val out = ListBuffer.empty[T]
        while (dataFileReader.hasNext) {
          dataFileReader.next(record)
          val t = avroReader.read(schema, record)
          out.append(t)
        }
        dataFileReader.close()
        IO.succeed(out.toList)
      }
    }

//  def readBinaryFile[T](file: File)(implicit schemaGenerator: SchemaGenerator[T], avroReader: AvroReader[T]): ZEnv[List[T]] =
//    ZIOFileUtils.managedFileInputStream(file).use { iss => readBinary(iss) }
}
