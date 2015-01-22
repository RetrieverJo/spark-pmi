name := "Scala-PMI"

version := "1.0"

scalaVersion := "2.11.5"

resolvers += "Apache Maven Central Repository" at "http://repo.maven.apache.org/maven2/"

assemblyJarName in assembly := "PMI.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.fluentd" % "fluent-logger-scala_2.11" % "0.5.1"
)