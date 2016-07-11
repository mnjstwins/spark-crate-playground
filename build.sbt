name := "spark-crate-playground"

version := "0.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided",
  "io.crate" % "crate-jdbc" % "1.12.2"
)

resolvers += Resolver.sonatypeRepo("public")
