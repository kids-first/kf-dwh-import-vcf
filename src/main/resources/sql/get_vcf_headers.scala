import io.projectglow.vcf.VCFFileFormat
import io.projectglow.sql.util.SerializableConfiguration
import scala.collection.JavaConverters._
import htsjdk.variant.vcf._
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.SparkHadoopUtil
val hadoopConf = spark.sessionState.newHadoopConf()
val hdfsPath = new Path("s3a://kf-study-us-east-1-prd-sd-dypmehhf/harmonized/family-variants/*.vcf.gz")

val fs = hdfsPath.getFileSystem(hadoopConf)
val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
val globPath = SparkHadoopUtil.get.globPathIfNecessary(fs, qualified).toSeq
val serializableConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
val filePaths = globPath.map(_.toString)
val p =  spark
  .sparkContext
  .parallelize(filePaths)
  .map { path =>
    val (header, _) = VCFFileFormat.createVCFCodec(path, serializableConf.value)
    path -> header.getInfoHeaderLines.asScala.toSet.filter(t => t.getID() == "ReadPosRankSum").headOption
  }
  .collect()
p.foreach(t => println(t))