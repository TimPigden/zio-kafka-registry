package zio.avro.magnolia

import org.apache.avro.Schema
import org.json4s.JValue
import org.json4s.jackson.JsonMethods._

object AvroCompiler {

  def compile(jv: JValue):Schema = {
    new Schema.Parser().parse(compact(jv))
  }

}
