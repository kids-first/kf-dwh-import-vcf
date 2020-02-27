import org.apache.spark.sql.{SaveMode, SparkSession}
spark.sql("use variant")
val ann = spark.table("annotations_sd_9pyzahhe_re_123456")

val genomes = spark.table("1000_genomes")
val topmed = spark.table("topmed_bravo")
val gnomad = spark.table("gnomad_genomes_2_1_1_liftover_grch38")

val join1k = ann
  .join(genomes, ann("chromosome") === genomes("chromosome") && ann("start") === genomes("start") && ann("reference") === genomes("reference") && ann("alternate") === genomes("alternate"), "left")
  .select(ann("*"), when(genomes("chromosome").isNull, lit(null)).otherwise(struct(genomes.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as "1k_genomes")

val joinTopmed = join1k.join(topmed, join1k("chromosome") === topmed("chromosome") && join1k("start") === topmed("start") && join1k("reference") === topmed("reference") && join1k("alternate") === topmed("alternate"), "left")
  .select(join1k("*"), when(topmed("chromosome").isNull, lit(null)).otherwise(struct(topmed.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as "topmed")

joinTopmed.join(gnomad, joinTopmed("chromosome") === gnomad("chromosome") && joinTopmed("start") === gnomad("start") && joinTopmed("reference") === gnomad("reference") && joinTopmed("alternate") === gnomad("alternate"), "left")
  .select(joinTopmed("*"), when(gnomad("chromosome").isNull, lit(null)).otherwise(struct(gnomad.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as "gnomad_2_1")
  .repartition($"chromosome")
  .sortWithinPartitions("start")
  .write
  .mode(SaveMode.Overwrite)
  .format("parquet")
  .option("path", "s3://kf-variant-parquet-prd/annotation/join_with_topmed_and_1k_and_gnomad")
  .saveAsTable("variant.join_with_topmed_and_1k_and_gnomad")