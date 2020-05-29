%pyspark
from pyspark.sql.functions import col,translate
from pyspark.sql.functions import col
import hail as hl

hl.init(sc=sc)
mt = hl.read_table('gs://gnomad-public/release/3.0/ht/genomes/gnomad.genomes.r3.0.sites.ht')
freq_dict=mt.freq_index_dict.collect()
# gnomad_freq_dict = {k:v for (k,v) in freq_dict[0].items() if k.startswith('adj')}

df=mt.to_spark()
# df.printSchema()

df=df.select(translate(col('`locus.contig`'), 'chr', '').alias('chromosome'), (col('`locus.position`')).alias('start'), col('alleles').getItem(0).alias('reference'), col('alleles').getItem(1).alias('alternate'), col('freq'))
for (k,v) in  freq_dict[0].items():
    for c in ['AC', 'AN', 'AF', 'homozygote_count']:
        columnName=k.replace('adj_', '') + '_' + c.lower().replace('homozygote_count', 'hom')
        columnName=columnName.replace('adj_', '')
        df=df.withColumn(columnName,df['freq'].getItem(v)[c])

df=df.drop('freq').where(col('af').isNotNull())
df.repartition("chromosome").sortWithinPartitions("start").write.mode('overwrite').format("parquet").option("path", "s3a://kf-variant-parquet-prd/public/gnomad/gnomad_genomes_3.0").saveAsTable("variant.gnomad_genomes_3_0")