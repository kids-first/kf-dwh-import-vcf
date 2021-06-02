
name := "kf-dwh-import-vcf"

scalaVersion := "2.12.13"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark_version = "3.1.1"

resolvers += "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"
resolvers += "Sonatype OSS Releases" at "https://s01.oss.sonatype.org/content/repositories/releases"

/* Runtime */
libraryDependencies += "org.apache.spark" %% "spark-sql" % spark_version % Provided
libraryDependencies += "io.projectglow" %% "glow" % "0.5.0" % Provided
libraryDependencies += "bio.ferlab" %% "datalake-spark3" % "0.0.45"

/* Test */
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version % "test"

parallelExecution in test := false
fork := true

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
  case "META-INF/DISCLAIMER" => MergeStrategy.last
  case "mozilla/public-suffix-list.txt" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last
  case "git.properties" => MergeStrategy.discard
  case "mime.types" => MergeStrategy.first
  case PathList("scala", "annotation", "nowarn.class" | "nowarn$.class") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
assemblyJarName in assembly := s"kf-dwh-import-vcf.jar"
