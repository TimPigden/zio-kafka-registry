package zio.kafka.registry.rest

import java.text.ParseException

import org.apache.avro.Schema
import org.json4s._
import org.json4s.jackson.JsonMethods._
import zio.kafka.registry.rest.RestClient.{CompatibilityLevel, SchemaError, WrappedSchema}
import zio._
/** serializers for kafka registry rest client.
 * NB, we assume that data coming from kafka registry is always in correct format
 * consequently our serializers fail with ParseException or equivalent for parsing json
 */
object Serializers {

  /**
   * {
   * "error_code": 422,
   * "message": "schema may not be empty"
   * }
   */
  implicit def parseSchemaError(s: String): Task[SchemaError] = IO.effect {
    (for {
      JObject(jobj) <- parse(s)
      JField("error_code", JInt(errorCode)) <- jobj
      JField("message", JString(message)) <- jobj
    } yield {
      println(s"got error stuff $s")
      SchemaError(errorCode.toInt, message)
    }).head
  }

  /** expecting something like this
   * {
   * "schema": "{\"type\": \"string\"}"
   * }
   */
  implicit def parseSchema(s: String): Task[Schema] = IO.effect {
    println(s"incoming schema $s")
    val schemaString: String = (for {
      JObject(jobj) <- parse(s)
      _ = println(s"jobj is $jobj")
      JField("schema", JString(schemaStrings)) <- jobj
      _ = println(s"schema is $schemaStrings")
    } yield schemaStrings).head
    new Schema.Parser().parse(schemaString)
  }

  def parseSchemaDirect(s: String): Task[Schema] = IO.effect {
    new Schema.Parser().parse(s)
  }

  /**
   * ["subject1", "subject2"]
   */
  implicit def parseSubjects(s: String): Task[List[String]] = IO.effect {
    (for {
      JArray(items) <- parse(s)
    } yield items).head.map { case JString(s) => s
    case _ => throw new ParseException("expected strings as subjects", 0)
    }
  }

  /**
   * [
   * 1, 2, 3, 4
   * ]
   */
  implicit def parseVersions(s: String): Task[List[Int]] = IO.effect {
    (for {
      JArray(items) <- parse(s)
    } yield items).head.map { case JInt(s) => s.toInt
    case _ => throw new ParseException("expected ints as versions", 0)
    }
  }

  implicit def parseWrappedSchema(s: String): Task[WrappedSchema] = parseSchema(s).map { schema =>
    (for {
      JObject(jobj) <- parse(s)
      JField("version", JInt(version)) <- jobj
      JField("subject", JString(subject)) <- jobj
      JField("id", JInt(id)) <- jobj
    } yield WrappedSchema(subject, id.toInt, version.toInt, schema)).head
  }

  implicit def parseId(s: String): Task[Int] = IO.effect {
    (for {
      JObject(jobj) <- parse(s)
      JField("id", JInt(id)) <- jobj
    } yield id.toInt).head
  }

  implicit def writeSchema(schema: Schema): String = {
    val asString = schema.toString
    val jv = JObject("schema" -> JString(asString))
    compact(jv)
  }

  implicit def writeCompatibilityLevel(compatibilityLevel: CompatibilityLevel): String = {
  val jv = JObject("compatibility" -> JString(CompatibilityLevel.values(compatibilityLevel)))
  compact(jv)
}

  def parseCompatibilityLevel(asPut: Boolean)(s: String): Task[CompatibilityLevel] = IO.effect {
    println(s"parseCompatibilityLevel $asPut of $s")
    val cString = if (asPut)
      "compatibility" else "compatibilityLevel"
    (for {
      JObject(jObj) <- parse(s)
      JField(cString, JString(level)) <- jObj
    } yield CompatibilityLevel.values.find(_._2 == level).get._1).head
  }

  implicit def parseIsCompatible(s: String):Task[Boolean] = IO.effect {
    (for {
      JObject(jobj) <- parse(s)
      JField("is_compatible", JBool(id)) <- jobj
    } yield id).head
  }

  implicit def parseString(s: String): Task[String] = IO.effectTotal(s)
}
