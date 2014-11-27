name := "kafkatest"

version := "1.0"

libraryDependencies ++= Seq(
    "org.apache.kafka" %% "kafka" % "0.8.2-beta" % "test",
    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "org.apache.curator" % "curator-test" % "2.7.0"
)

