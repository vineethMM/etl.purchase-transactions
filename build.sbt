val projectName = "etl-purchase-transactions"

val sparkVersion = "2.4.5" // Stable latest version of Spark

val dependencies = Seq(
  // main
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "com.github.nscala-time" %% "nscala-time" % "2.16.0",
  // test
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

lazy val main = Project(projectName, base = file("."))
  .settings(libraryDependencies ++= dependencies)
  .settings(scalaVersion := "2.11.8") // Scala version compatible with Spark 2.4.5