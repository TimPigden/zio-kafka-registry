package zio.kafka.registry.rest

import java.text.ParseException

import org.apache.avro.Schema
import org.http4s.blaze.http.parser.BaseExceptions.ParserException
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
    println(s"parseChemaError $s")
    (for {
      JObject(jobj) <- parse(s)
      JField("error_code", JInt(errorCode)) <- jobj
      JField("message", JString(message)) <- jobj
    } yield SchemaError(errorCode.toInt, message)).head
  }

  /** expecting something like this
   * {
   * "schema": "{\"type\": \"string\"}"
   * }
   */
  implicit def parseSchema(s:String): Task[Schema] = IO.effect {
    val schemaString: String = (for {
      JObject(jobj) <- parse(s)
      JField("schema", JString(schemaStrings)) <- jobj
    } yield schemaStrings).head
    new Schema.Parser().parse(schemaString)
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

  implicit def parseWrappedSchema(s:String): Task[WrappedSchema] = parseSchema(s).map { schema =>
    println(s"parsedWrappedSchmea $s")
      (for {
        JObject(jobj) <- parse(s)
        JField("version", JInt(version)) <- jobj
        JField("subject", JString(subject)) <- jobj
        JField("id", JInt(id)) <- jobj
      } yield WrappedSchema(subject, id.toInt, version.toInt, schema)).head
  }

  implicit def parseId(s: String):Task[Int] = IO.effect {
    (for {
      JObject(jobj) <- parse(s)
      JField("id", JInt(id)) <- jobj
    } yield id.toInt).head
  }

  implicit def writeSchema(schema: Schema): String = {
    val asString = schema.toString
    val jv = JObject("schema"-> JString(asString))
    compact(jv)
  }

  implicit def writeCompatibilityLevel(compatibilityLevel: CompatibilityLevel): String =
    CompatibilityLevel.values(compatibilityLevel)

  implicit def parseCompatibilityLevel(s: String): Task[CompatibilityLevel] = IO.effect {
    CompatibilityLevel.values.find(_._2 == s).get._1
  }

  implicit def parseIsCompatible(s: String):Task[Boolean] = IO.effect {
    (for {
      JObject(jobj) <- parse(s)
      JField("id", JBool(id)) <- jobj
    } yield id).head
  }

  implicit def parseString(s: String): Task[String] = IO.succeed(s)
}