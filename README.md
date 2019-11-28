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

For actual rest api calls see the (source code)[]




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

# Limitations
* Avro serdes for keys not supported.
* Only one schema url supported.
* Not tested on production systems (I don't have any).
