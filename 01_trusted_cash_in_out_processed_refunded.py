import pyspark.sql.functions as F
import pyspark.sql.types as T

spark.sparkContext.setLogLevel("ERROR")

### Defining path
path = '/home/rafaelrs/Documents/case_stone/data'
relative_read_path = f'{path}/data_engineer_challenge_dataset/'
relative_write_path = f'{path}/transaction_history/'

### Reading the dataframes 
cash_data_schema = T.StructType([
    T.StructField('account_id',T.StringType(),True),
    T.StructField('amount',T.LongType(),True),
    T.StructField('counterparty_account_id',T.StringType(),True),
    T.StructField('inserted_at',T.LongType(),True),
    T.StructField('transaction_id',T.StringType(),True)])

processing_data_schema = T.StructType([
     T.StructField('inserted_at',T.LongType(),True),
     T.StructField('transaction_id',T.StringType(),True)])

cash_in_df = (spark
    .readStream.schema(cash_data_schema)
    .format('json')
    .option('maxFilesPerTrigger',1)
    .load(f"{relative_read_path}{'cash_ins/'}")
)

created_df = (spark
    .readStream.schema(cash_data_schema)
    .format('json')
    .option('maxFilesPerTrigger',1)
    .load(f"{relative_read_path}{'cash_outs_created/'}")
)

processed_df = (spark
    .readStream.schema(processing_data_schema)
    .format('json')
    .option('maxFilesPerTrigger',1)
    .load(f"{relative_read_path}{'cash_outs_processed/'}")
)

refunded_df = (spark
    .readStream.schema(processing_data_schema)
    .format('json')
    .option('maxFilesPerTrigger',1)
    .load(f"{relative_read_path}{'cash_outs_refunded/'}")
)


### Transformation
created_df = created_df.withColumnRenamed('inserted_at', 'created_time')
created_df = created_df.withColumnRenamed('transaction_id', 'created_id')

#### Selecting records created and processed & records created and refunded
created_processed_df = created_df.join(
    processed_df,
    (F.col('transaction_id') == F.col('created_id')) & 
    ((F.col('inserted_at') > F.col('created_time'))),
    'inner'
)

created_processed_df = created_processed_df.withColumnRenamed('transaction_id', 'cp_transaction_id')
created_processed_df = created_processed_df.withColumnRenamed('inserted_at', 'cp_inserted_at')

created_processed_refunded_df = created_processed_df.join(
    refunded_df,
    (F.col('transaction_id') == F.col('cp_transaction_id')) & 
    ((F.col('inserted_at') > F.col('cp_inserted_at'))),
    'inner'
)

created_processed_df = created_processed_df.drop('created_time', 'created_id')
created_processed_df = created_processed_df.withColumnRenamed('cp_inserted_at', 'inserted_at')
created_processed_df = created_processed_df.withColumnRenamed('cp_transaction_id', 'transaction_id')
created_processed_df = created_processed_df.withColumn('amount', - F.col('amount'))

created_processed_refunded_df = created_processed_refunded_df.drop('created_time', 'created_id', 'cp_inserted_at', 'cp_transaction_id')

created_processed_refunded_df = (created_processed_df
    .select(created_processed_refunded_df.columns)
    .union(created_processed_refunded_df))

created_df = created_df.withColumnRenamed('created_time', 'inserted_at')
created_df = created_df.withColumnRenamed('created_id', 'transaction_id')
created_df = created_df.withColumn('amount', F.lit(None))

### Transaction History zone
transaction_history_df = (cash_in_df.select(created_processed_refunded_df.columns)
    .union(created_processed_refunded_df)
    .union(created_df.select(created_processed_refunded_df.columns)))

### Write zone
(transaction_history_df
    .writeStream
    .outputMode("append")
    .format("parquet")
    .start(relative_write_path)
    .awaitTermination()
)