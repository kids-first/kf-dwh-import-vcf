
name := "kf-dwh-import-vcf"

version := "0.1"

scalaVersion := "2.11.12"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark_version = "2.4.5"

/* Runtime */
libraryDependencies +=  "org.apache.spark" %% "spark-sql" % spark_version % Provided
libraryDependencies += "io.projectglow" %% "glow" % "0.3.0" % Provided

/* Test */
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version % "test"


test in assembly := {}

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
  case "META-INF/DISCLAIMER" => MergeStrategy.last
  case "mozilla/public-suffix-list.txt" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last
  case "git.properties" => MergeStrategy.discard
  case "mime.types" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
assemblyJarName in assembly := "kf-dwh-import-vcf.jar"
