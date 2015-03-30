name := """counting_artists"""

version := "1.0"

scalaVersion := "2.11.5"

mainClass in (Compile,run) := Some("com.example.ArtistCoocurrence")

libraryDependencies ++= Seq(
  // Uncomment to use Akka
  //"com.typesafe.akka" % "akka-actor_2.11" % "2.3.9",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1"
)
