
name := "kf-dwh-import-vcf"

scalaVersion := "2.12.13"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark_version = "3.1.2"
val deltaCoreVersion = "1.0.0"
val elasticsearch_spark_version = "7.12.0"
val scalatestVersion = "3.2.9"
val datalakeSpark3Version = "0.0.54"

resolvers += "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"
resolvers += "Sonatype OSS Releases" at "https://s01.oss.sonatype.org/content/repositories/releases"

/* Runtime */
libraryDependencies += "org.apache.spark" %% "spark-sql" % spark_version % Provided
libraryDependencies += "io.delta" %% "delta-core" % deltaCoreVersion
libraryDependencies += "bio.ferlab" %% "datalake-spark3" % datalakeSpark3Version
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-30" % elasticsearch_spark_version % Provided

/* Test */
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % Test
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version % Test

test / parallelExecution := false
fork := true

assembly / test := {}

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inAll
)

assembly / assemblyMergeStrategy := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
  case "META-INF/DISCLAIMER" => MergeStrategy.last
  case "mozilla/public-suffix-list.txt" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last
  case "git.properties" => MergeStrategy.discard
  case "mime.types" => MergeStrategy.first
  case PathList("scala", "annotation", "nowarn.class" | "nowarn$.class") => MergeStrategy.first
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
assembly / assemblyJarName := s"kf-dwh-import-vcf.jar"
