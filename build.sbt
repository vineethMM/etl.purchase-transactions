val projectName = "etl-purchase-transactions"

val sparkVersion = "3.0.0-preview2"

val dependencies = Seq(
  // main
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  // test
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"

)

lazy val main = Project(projectName, base = file("."))
  .settings(libraryDependencies ++= dependencies)
  .settings(scalaVersion := "2.12.10")