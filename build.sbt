lazy val root = (project in file(".")).
settings(
    name := "Text2Hive",
    version := "0.1",
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("Launch")    
        )

libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-common" % "2.7.3",
    "org.apache.hadoop" % "hadoop-client" % "2.7.3",
    "com.opencsv" % "opencsv" % "3.8",
    "org.scala-lang.modules" %% "scala-xml" % "1.0.2",
    "org.apache.hive" % "hive-jdbc" % "1.2.1"
    //"org.apache.hadoop" % "hadoop-hdfs" % "2.7.3"
    )

assemblyMergeStrategy in assembly := {
     case PathList("META-INF", xs @ _*) => MergeStrategy.discard
          case x => MergeStrategy.first

}

