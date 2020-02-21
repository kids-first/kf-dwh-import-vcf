val clinvar = spark.read.format("vcf").option("splitToBiallelic", "true").load("s3://kf-variant-parquet-prd/raw/clinvar/clinvar_20200217.vcf.gz")
  .where($"splitFromMultiAllelic" === lit(false))
  .drop("qual", "filters", "splitFromMultiAllelic", "genotypes")
z.show(clinvar)