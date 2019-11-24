package zio.kafka.registry.rest

import zio.{RIO, Task}

trait AbstractClient[-R] {

  def get[T](url: String)(implicit tParser: String => Task[T]): RIO[R, T]

  def post[In, Out](url: String, in: In)(implicit inWriter: In => String, outParser: String => Task[Out])
  : RIO[R, Out]

  def put[In, Out](url: String, in: In)(implicit inWriter: In => String, outParser: String => Task[Out])
  : RIO[R, Out]

  def delete[Out](url: String)(implicit outParser: String => Task[Out]): RIO[R, Out]

}
