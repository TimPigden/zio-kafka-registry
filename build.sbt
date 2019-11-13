organization := "org.timpigden"

name := "zio-kafka-registry"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

inThisBuild(Seq(
  version := "0.1.0",
  isSnapshot := true,
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
  "-language:implicitConversions"
  )

val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
val json4s =  "org.json4s" %% "json4s-jackson" % "3.6.6"

val cats = "org.typelevel" %% "cats-core" % "2.0.0"
val catsEffect =  "org.typelevel" %% "cats-effect" % "2.0.0"
val squants = "org.typelevel"  %% "squants"  % "1.3.0"

// val magnolia = "com.softwaremill" %% "magnolia" % "0.11.0-sml"
val magnolia = "com.propensive" %% "magnolia" % "0.12.0"
lazy val `zio-version` = "1.0.0-RC16"
lazy val `zio-kafka-version` = "0.4.0" // dummy version to pick up local library
lazy val zio = "dev.zio" %% "zio" %  `zio-version`
lazy val `zio-kafka` = "dev.zio" %% "zio-kafka"   % `zio-kafka-version`
lazy val `zio-test` = "dev.zio" %% "zio-test" % `zio-version` % "test"
lazy val `zio-test-sbt` = "dev.zio" %% "zio-test-sbt" % `zio-version` % "test"
lazy val `embedded-kafka-schema` = "io.github.embeddedkafka" %% "embedded-kafka-schema-registry" % "5.3.1" % "test"
lazy val `embedded-kafka` = "io.github.embeddedkafka" %% "embedded-kafka" % "2.3.1" % "test"

lazy val avro = "org.apache.avro" % "avro" % "1.9.1"
lazy val snappy = "org.xerial.snappy" % "snappy-java" % "1.1.7.3"
lazy val confluentSerializer = "io.confluent" % "kafka-avro-serializer" % "5.3.1"

lazy val commonSettings = Seq(
  parallelExecution in Test := false,
  scalacOptions ++= myScalacOptions,
  organization := "com.optrak",
)

lazy val `zio-avro` = (project in file ("zio-avro"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Seq(
    json4s,
    avro,
    zio,
    `zio-test`,
    snappy,
    magnolia,
  )
)
lazy val `zio-kafka-registry` = (project in file ("zio-kafka-registry"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Seq(
    json4s,
    avro,
    zio,
    `zio-test`,
    `zio-kafka`,
    `embedded-kafka-schema`,
    snappy,
    confluentSerializer,
    magnolia,
  )

  )
    .dependsOn(`zio-avro`)

parallelExecution in Test := false

