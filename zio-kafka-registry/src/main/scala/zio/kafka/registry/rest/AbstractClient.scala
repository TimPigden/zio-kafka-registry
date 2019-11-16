package zio.kafka.registry.rest

import zio.Task
import zio.kafka.registry.rest.RestClient.RestResponse

trait AbstractClient {

  def get[T](url: String)(implicit tParser: String => Task[T]): RestResponse[T]

  def post[In, Out](url: String, in: In)(implicit inWriter: In => String, outParser: String => Task[Out])
  : RestResponse[Out]

  def put[In, Out](url: String, in: In)(implicit inWriter: In => String, outParser: String => Task[Out])
  : RestResponse[Out]

  def delete[Out](url: String)(implicit outParser: String => Task[Out]): RestResponse[Out]

}
