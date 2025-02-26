file_name=$1
conf_value=$2

# Run spark-submit with the configuration and the Python script
/usr/lib/spark/bin/spark-submit --driver-memory 3G --executor-cores 3 --executor-memory 10G --conf spark.driver.allowMultipleContexts=true --conf spark.scheduler.mode=FAIR --conf spark.sql.shuffle.repartitions=500 --conf spark.yarn.executor.memoryOverhead=4000M --conf spark.default.parallelism=700 --conf spark.port.maxRetries=200 --conf spark.rpc.askTimeout=600s --conf spark.network.timeout=10000000 tascanCSVToParquet.py $file_name
