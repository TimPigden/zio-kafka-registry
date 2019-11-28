# zio-kafka-registry

Project to use zio-kafka in combination with the Confluent Schema Registry.

# Overview
This library provides 2 main functions
1. It allows you to call the Confluent Kafka Schema Registry Rest API with pretty well all the options.
2. It allows you to produce and consume Avro data using the schema registry to manage the schemas.

For those of you using the kafka-avro-serializer and kafka-schema-registry libraries, it's a fairly thing zio wrapper on those, making use of the
[avro4s](https://github.com/sksamuel/avro4s]) library to convert to and from case classes, schemas and Avro GenericRecord.

# Rest API
The Rest API adheres fairly closely to the (actual API)[https://docs.confluent.io/current/schema-registry/develop/api.html].

It uses the Module pattern
```
/**
 * Module pattern for ConfluentClientService
 */
trait ConfluentClient {
  def confluentClient: ConfluentClientService
}
```
But the service can be created from the companion object
```scala

object ConfluentClientService {

  /**
   * creates a ConfluentClientService
   * @param urlIn the single url used to find the registry
   * @param identityMapCapacity number if distinct schemas to cache
   * @return the service
   */

  def create(urlIn: String,
             identityMapCapacity: Int): Task[ConfluentClientService]
```
or the ConfluentClient as a Zio Managed
```
object ConfluentClient {

  /**
   * creates a ConfluentClient as ZManaged
   * @param url the single url used to find the registry
   * @param identityMapCapacity number if distinct schemas to cache
   * @return the service
   */

  def make[R <: Blocking](url: String,
                          identityMapCapacity: Int): ZManaged[R, Throwable, ConfluentClient] =

```
Note that pretty well everything requires Blocking as they all use or may use confluent's rest client

For actual rest api calls see the (source code)[https://github.com/TimPigden/zio-kafka-registry/blob/master/zio-kafka-registry/src/main/scala/zio/kafka/registry/ConfluentClientService.scala] or scala docs. An example is here:
```
trait ConfluentClientService {
// ...
  /**
   * gets a specific schema
   * @param subject subject name
   * @param versionId if versionId = None uses latest
   * @return with metadata (as WrappedSchema). Error if version Id does not exist or if versionId is None, there is
   *         no latest
   */
  def version(subject: String, versionId: Option[Int]): RestResponse[WrappedSchema] =```
```

# Security
The following methods are accessible on the ConfluentRestClient:
```
  def setSslSocketFactory(sslSocketFactory: SSLSocketFactory): RestConfigResponse[Unit] = 

  def setBasicAuthCredentialProvider(basicAuthCredentialProvider: BasicAuthCredentialProvider) : RestConfigResponse[Unit] =

  def setBearerAuthCredentialProvider(bearerAuthCredentialProvider: BearerAuthCredentialProvider) : RestConfigResponse[Unit] =
  
  /** http headers to go to schema registry server  */  
  def setHttpHeaders(httpHeaders: util.Map[String, String]) : RestConfigResponse[Unit] =
```
They directly call the underlying client code from io.confluent.kafka.schemaregistry.client.rest.RestService

To use them create a ConfluentClientService and call the method on confluentRestService object. e.g.
```
confluentRestClient.confluentRestService.setSslSocketFactory(mySocketFactory)
```

# Producing and consuming records
Producing and consuming follows the same pattern as for (zio-kafka)[https://github.com/zio/zio-kafka] libray. Full code examples are found in the tests.

Note this is test code - hence the slightly complicated liveClockBlocking - to ensure that the calls take place with the zio-test Live environment.

```
 def withProducer[A, K, V](    kSerde: Serializer[Any, K],
                                vSerde: Serializer[Any with Blocking, V]
                           )(r: Producer[Any with Blocking, K, V] => RIO[Any with Clock with Kafka with Blocking, A]) =
    for {
      settings <- registryProducerSettings
      producer = Producer.make(settings, kSerde, vSerde)
      lcb      <- Kafka.liveClockBlocking
      produced <- producer.use { p =>
                   r(p).provide(lcb)
                 }
    } yield produced

  def withProducerAvroRecord[A, V](recordFormat: RecordFormat[V])
                                  (r: Producer[Any with Blocking, String, GenericRecord] => RIO[Any with Clock with Kafka with Blocking, A]
                                  ) = {
    for {
      client <-  ZIO.environment[ConfluentClient]
      serializer <- client.confluentClient.avroSerializer(RecordNameStrategy)
      producing <-  withProducer[A, String, GenericRecord](Serde.string, serializer)(r)
    } yield producing
  }
  
  def produceMany[T](t: String, kvs: Iterable[(String, T)])
                    (implicit recordFormat: RecordFormat[T]) =
    withProducerAvroRecord(recordFormat) { p =>
      val records = kvs.map {
        case (k, v) =>
          val rec = recordFormat.to(v)
          new ProducerRecord[String, GenericRecord](t, k, rec)
      }
      val chunk = Chunk.fromIterable(records)
      p.produceChunk(chunk)
    }.flatten
```

For consuming:
```
  def withConsumer[A : ClassTag](groupId: String, clientId: String)(
    r: Consumer => RIO[Any with Kafka with Clock with Blocking, A]) =
    for {
      lcb <- Kafka.liveClockBlocking
      inner <- (for {
                settings <- consumerSettings(groupId, clientId)
                consumer = Consumer.make(settings)
                consumed <- consumer.use { p =>
                             r(p).provide(lcb)
                           }
              } yield consumed).provide(lcb)
    } yield inner
    
object TestProducerSupport{
  val topic = "presidents"

  val presidents = List(President2("Lincoln", 1860),
    President2("Obama", 2008),
    President2("Trump", 2016))

  val testPresident: ZSpec[KafkaTestEnvironment, Throwable, String, Unit] = testM("test define and store president") {
    for {
      restClient <- ZIO.environment[ConfluentClient]
      _ <- produceMany(topic, presidents.map (p => "all" -> p))(format2)
      subjects <- restClient.confluentClient.subjects
      deserializer <- restClient.confluentClient.avroGenericDeserializer[President2](RecordNameStrategy)(format2)
      records <- withConsumer("any", "client") { consumer =>
        consumer
          .subscribeAnd(Subscription.Topics(Set(topic)))
          .plainStream(Serde.string, deserializer)
          .flattenChunks
          .take(3)
          .runCollect
      }
      vOut = records.map{_.record.value}
    } yield {
      println(s"got these subjects $subjects")
      assert(vOut, equalTo(presidents))
    }
  }
}

```

# Testing
If you want to write unit tests you should probably test against (embeddedkafka.schemaregistry)[https://github.com/embeddedkafka/embedded-kafka-schema-registry]  which enables you to start and stop
a Kafka server including registry.

The file (KafkaRegistryTestUtils)[https://github.com/TimPigden/zio-kafka-registry/blob/master/zio-kafka-registry/src/test/scala/zio/kafka/registry/KafkaRegistryTestUtils.scala] in the test code for this project is a good starting point and it may be easiest just to copy it into your own environment and use it directly or modify it.

Note that the testing is slightly convoluted since we need the provideManagedShared to provide single copies of the managed embedded kafka as well as the client - they are both heavy-weight objects. See zio-test docs for an explanation.

# Limitations
* Avro serdes for keys not supported.
* Only one schema url supported.
* Not tested on production systems (I don't have any).
