%pyspark
from pyspark.sql.functions import col,translate

import hail as hl

hl.init(sc=sc)

mt = hl.read_table('s3a://kf-variant-parquet-prd/raw/gnomad/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht')
freq_dict=mt.freq_index_dict.collect()
gnomad_freq_dict = {k:v for (k,v) in freq_dict[0].items() if k.startswith('gnomad') and not k.startswith('gnomad_raw')}
df=mt.to_spark()
df=df.select(translate(col('`locus.contig`'), 'chr', '').alias('chromosome'), (col('`locus.position`')).alias('start'), col('alleles').getItem(0).alias('reference'), col('alleles').getItem(1).alias('alternate'), col('freq'))
for (k,v) in gnomad_freq_dict.items():
    for c in ['AC', 'AN', 'AF', 'homozygote_count']:
        columnName=k.replace('gnomad_', '') + '_' + c.lower().replace('homozygote_count', 'hom')
        columnName=columnName.replace('gnomad_', '')
        df=df.withColumn(columnName,df['freq'].getItem(v)[c])

df=df.drop('freq').where(col('af').isNotNull())
df.repartition("chromosome").sortWithinPartitions("start").write.mode('overwrite').format("parquet").option("path", "s3a://kf-variant-parquet-prd/public/gnomad/gnomad_genomes_2.1.1_liftover_grch38").saveAsTable("variant.gnomad_genomes_2_1_1_liftover_grch38")