name := "kafka"

version := "1.0"

scalaVersion := "2.10.4"

//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.2");
//mainClass in assembly := Some("Kafka")

resolvers += "boundless" at "https://repo.boundlessgeo.com/artifactory/main/"
resolvers += "imageio" at "http://maven.geo-solutions.it"
resolvers += "osgeo" at "http://download.osgeo.org/webdav/geotools"
resolvers += "boundless2" at "http://repo.boundlessgeo.com/main"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-streaming-kafka-assembly" % "1.5.0",
"org.apache.spark" %% "spark-core" % "1.5.0"  % "provided",
"org.apache.spark" %% "spark-streaming" % "1.5.0"  % "provided",
"com.typesafe.play" %% "play-json" % "2.4.10",
"org.joda" % "joda-convert" % "1.8.1",
"org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
"com.vividsolutions" % "jts" % "1.13",
"org.geotools" % "gt-main" % "17.0",
"jgridshift" % "jgridshift" % "1.0",
"org.geotools" % "gt-epsg-hsql" % "17.0"
)

scalacOptions ++= Seq("-deprecation");

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

mainClass in assembly := Some("aw.kafka");

assemblyMergeStrategy in assembly := {
    //case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case PathList("org","apache","spark","unused","UnusedStubClass.class") => MergeStrategy.discard
    case x => MergeStrategy.defaultMergeStrategy(x)
}

/*
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}*/


//"org.apache.spark" %% "spark-streaming-kafka" % "1.5.0"  % "provided",
