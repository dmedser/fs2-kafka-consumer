name := "fs2-kafka-consumer"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= {

  object Version {
    val cats = "2.1.1"
    val catsEffect = "2.1.3"
    val fs2Kafka = "1.0.0"
    val logback = "1.2.3"
    val log4Cats = "1.0.1"
    val kindProjector = "0.11.0"
    val betterMonadicFor = "0.3.1"
  }

  val catsCore = "org.typelevel"          %% "cats-core"      % Version.cats
  val catsEffect = "org.typelevel"        %% "cats-effect"    % Version.catsEffect
  val fs2Kafka = "com.github.fd4s"        %% "fs2-kafka"      % Version.fs2Kafka
  val logback = "ch.qos.logback"          % "logback-classic" % Version.logback
  val log4CatsSlf4j = "io.chrisdavenport" %% "log4cats-slf4j" % Version.log4Cats
  val kindProjector =
    compilerPlugin("org.typelevel"                   %% "kind-projector"     % Version.kindProjector cross CrossVersion.full)
  val betterMonadicFor = compilerPlugin("com.olegpy" %% "better-monadic-for" % Version.betterMonadicFor)

  List(catsCore, catsEffect, fs2Kafka, logback, log4CatsSlf4j, kindProjector, betterMonadicFor)
}

resolvers ++= List(Resolver.sonatypeRepo("releases"), Resolver.bintrayRepo("krasserm", "maven"))
