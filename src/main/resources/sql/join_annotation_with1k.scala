val ann = spark.table("annotations_sd_9pyzahhe_re_123456")

val genomes = spark.table("1000_genomes")

ann
  .join(genomes, ann("chromosome") === genomes("chromosome") && ann("start") === genomes("start") && ann("reference") === genomes("reference") && ann("alternate") === genomes("alternate"), "left")
  .select(ann("*"), when(genomes("chromosome").isNull, lit(null)).otherwise(struct(genomes.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as "1k_genomes")
  .repartition($"chromosome")
  .sortWithinPartitions("start")
  .write
  .mode(SaveMode.Overwrite)
  .format("parquet")
  .option("path", s"$output/annotation/join_with_1k")
  .saveAsTable("variant.annotation_join_with_1k")
