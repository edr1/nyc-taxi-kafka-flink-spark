import glob
from pathlib import Path
from pyspark.sql import SparkSession,functions as F,Window
Path('/app/results/spark-events').mkdir(parents=True, exist_ok=True)
spark=SparkSession.builder.appName('NYC Taxi Historical Batch').master('local[*]').config('spark.sql.session.timeZone','America/New_York').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
files=sorted(glob.glob('/app/data/historical/*.parquet'))
assert len(files)==3, 'Se requieren los tres Parquet de enero a marzo de 2024'
taxi=spark.read.parquet(*files)
z=spark.read.option('header',True).option('inferSchema',True).csv('/app/data/reference/taxi_zone_lookup.csv')
clean=taxi.filter(F.col('PULocationID').between(1,263) & (F.col('total_amount')>=0) & (F.col('trip_distance')>=0)).filter((F.col('tpep_pickup_datetime')>=F.lit('2024-01-01')) & (F.col('tpep_pickup_datetime')<F.lit('2024-04-01')))
joined=clean.join(F.broadcast(z.select(F.col('LocationID').alias('PULocationID'),'Borough','Zone')),'PULocationID','left')
agg=joined.groupBy('Borough','Zone','PULocationID').agg(F.count('*').alias('trips'),F.round(F.sum('total_amount'),2).alias('revenue'))
rank=agg.withColumn('rank',F.row_number().over(Window.partitionBy('Borough').orderBy(F.desc('trips'),F.asc('PULocationID'))))
out='/app/results/batch'
def save(df,name):df.coalesce(1).write.mode('overwrite').option('header',True).csv(out+'/'+name)
save(agg.orderBy(F.desc('trips')),'p1_zones')
save(agg.groupBy('Borough').agg(F.sum('trips').alias('trips'),F.round(F.sum('revenue'),2).alias('revenue')).orderBy(F.desc('trips')),'p1_boroughs')
save(rank.filter('rank<=3').orderBy('Borough','rank'),'p7_top3')
save(joined.groupBy(F.hour('tpep_pickup_datetime').alias('pickup_hour'),F.dayofweek('tpep_pickup_datetime').alias('weekday_num')).agg(F.count('*').alias('trips')).orderBy('weekday_num','pickup_hour'),'p2_hour_weekday')
# Open-Meteo export includes two metadata lines and a blank line before actual CSV header.
# Spark CSV skipRows support varies; read using text and parse CSV after dropping metadata.
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql import Row
raw=spark.sparkContext.textFile('/app/data/weather/open-meteo-40.74N74.04W32m.csv')
lines=raw.zipWithIndex().filter(lambda x:x[1]>=4 and bool(x[0].strip())).map(lambda x:x[0].split(','))
w=spark.createDataFrame(lines.map(lambda a:Row(time=a[0],temperature_c=float(a[1]),precipitation_mm=float(a[2]),rain_mm=float(a[3]))))
w=w.withColumn('weather_hour',F.to_timestamp('time',"yyyy-MM-dd'T'HH:mm"))
with_weather=joined.withColumn('pickup_hour_ts',F.date_trunc('hour','tpep_pickup_datetime')).join(F.broadcast(w),F.col('pickup_hour_ts')==F.col('weather_hour'),'inner')
save(with_weather.groupBy(F.when(F.col('rain_mm')>0,'rain').otherwise('no_rain').alias('rain_group')).agg(F.count('*').alias('trips'),F.round(F.avg('trip_distance'),2).alias('avg_distance_miles'),F.round(F.avg('tip_amount'),2).alias('avg_tip_usd')),'weather_exploratory')
print('RESULTADOS HISTORICOS P1');agg.orderBy(F.desc('trips')).show(10,False)
print('RESULTADOS HISTORICOS P7');rank.filter('rank<=3').orderBy('Borough','rank').show(30,False)
spark.stop()
