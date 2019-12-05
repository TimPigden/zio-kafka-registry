organization := "org.timpigden"

name := "zio-kafka-registry"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

inThisBuild(Seq(
  version := "0.1.1",
  isSnapshot := false,
  scalaVersion := "2.12.10",
  resolvers += "confluent" at "https://packages.confluent.io/maven/",
  useCoursier := false
))

resolvers += Resolver.sonatypeRepo("releases")

inThisBuild(
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")
)

val myScalacOptions = Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-target:jvm-1.8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-language:postfixOps",
  "-language:higherKinds",
  "-Ywarn-value-discard",
  "-Ypartial-unification",
  "-language:implicitConversions",
  "-Xlint"
  )

val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
val json4s =  "org.json4s" %% "json4s-jackson" % "3.6.6"

val cats = "org.typelevel" %% "cats-core" % "2.0.0"
val catsEffect =  "org.typelevel" %% "cats-effect" % "2.0.0"
val squants = "org.typelevel"  %% "squants"  % "1.3.0"

// val magnolia = "com.softwaremill" %% "magnolia" % "0.11.0-sml"
val magnolia = "com.propensive" %% "magnolia" % "0.12.0"
lazy val zioVersion           = "1.0.0-RC17+94-fa044641"
lazy val zioKafkaVersion           = "0.4.1+34-69f19fa2"
lazy val embeddedKafkaVersion = "2.4.0-SNAPSHOT"
lazy val embeddedKafkaRegistryVersion = "5.4.0-SNAPSHOT"
lazy val zio = "dev.zio" %% "zio" %  `zioVersion`
lazy val `zio-kafka` = "dev.zio" %% "zio-kafka"   % zioKafkaVersion
lazy val `zio-test` = "dev.zio" %% "zio-test" % `zioVersion` % "test"
lazy val `zio-test-sbt` = "dev.zio" %% "zio-test-sbt" % `zioVersion` % "test"
lazy val `embedded-kafka-schema` = "io.github.embeddedkafka" %% "embedded-kafka-schema-registry" % embeddedKafkaRegistryVersion % "test"
lazy val `embedded-kafka-streams` = "io.github.embeddedkafka" %% "embedded-kafka-schema-streams" % embeddedKafkaVersion % "test"
lazy val kafkaVersion         = "2.3.1"

lazy val avro = "org.apache.avro" % "avro" % "1.9.1"
lazy val avro4s = "com.sksamuel.avro4s" %% "avro4s-core" % "3.0.4"
lazy val snappy = "org.xerial.snappy" % "snappy-java" % "1.1.7.3"
lazy val confluentSerializer = "io.confluent" % "kafka-avro-serializer" % "5.3.1"
lazy val confluentRegistry = "io.confluent" % "kafka-schema-registry" % "5.3.1"
lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaVersion
lazy val kafka = "org.apache.kafka" %% "kafka" % kafkaVersion

lazy val commonSettings = Seq(
  parallelExecution in Test := false,
  scalacOptions ++= myScalacOptions,
  organization := "org.timpigden",
)

lazy val `zio-kafka-registry` = (project in file ("zio-kafka-registry"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Seq(
    json4s,
    avro,
    zio,
    kafka,
    `zio-test`,
    `zio-kafka`,
    avro4s,
    `embedded-kafka-schema`,
    snappy,
    confluentSerializer,
    confluentRegistry
  )

  )

parallelExecution in Test := false

