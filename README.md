#tf

交通预测

数据 -> kafkaproduce -> kafka -> sparkstreaming  -> ETL 处理 -> Redis  -> spark Mlib   ->  mode  ->  hdfs   -> modepath -> redis  -> test


tf_produce -> 数据

tf_consumer -> ETL处理数据存入Redis

tf_modeling -> 读取Redis数据训练模型，把模型存入hdfs，模型路径存入Redis

tf_prediction -> 读取Redis模型路径，从HDFS读取模型，预测交通
