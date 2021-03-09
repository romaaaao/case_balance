try:
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    from pyspark.sql.window import Window
    import pandas as pd
except ImportError as e:
    print("It wasn't possible import packages. Error:", e)

path = '/home/rafaelrs/Documents/case_stone/data'
relative_read_path = f'{path}/transaction_history/'
relative_write_path = f'{path}/balance/'

balance_schema = T.StructType([
    T.StructField('account_id',T.StringType(),True),
    T.StructField('amount',T.LongType(),True),
    T.StructField('counterparty_account_id',T.StringType(),True),
    T.StructField('inserted_at',T.LongType(),True),
    T.StructField('transaction_id',T.StringType(),True)])

try:
    balance = spark.read.parquet(relative_read_path, schema=balance_schema)
except Exception as e:
    print("Something went wrong with the transaction history path", e)

by_account_id = Window.partitionBy(['account_id']).orderBy("inserted_at")

balance = balance.withColumn('balance', F.sum(F.col('amount')).over(by_account_id))

balance = balance.withColumn('inserted_at', F.to_timestamp(F.from_unixtime(F.col('inserted_at')/1000)))

balance.write.parquet(relative_write_path)