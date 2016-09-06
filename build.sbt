lazy val root = (project in file(".")).
settings(
    name := "Text2Hive",
    version := "0.1",
    scalaVersion := "2.11.8"                      
        )

libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-common" % "2.7.3",
    "org.apache.hadoop" % "hadoop-client" % "2.7.3",
    "com.opencsv" % "opencsv" % "3.8",
    "org.scala-lang.modules" %% "scala-xml" % "1.0.2",
    "org.apache.hive" % "hive-jdbc" % "1.2.1"
    )



