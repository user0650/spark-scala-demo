# UDAF

### 自定义聚合函数

使用Scala/Java语言开发UDAF，参考CollectBloomFilter

### 自定义函数的使用

##### Scala中使用：
```scala
spark.udf.registerJavaUDAF("collect_bf", "org.apache.spark.sql.udaf.CollectBloomFilter")
```
  直接注册即可使用，参考CollectBloomFilterDemo。


##### PySpark中使用：

- 首先，将scala/java编写的udaf打成jar包
- 然后，启动pyspark时，指定jar包
```python
pyspark --jars /Users/xxx/udaf.jar
```
- 最后，与scala中相同的方式使用即可
```python
# 注册：
spark.udf.registerJavaUDAF("collect_bf", "org.apache.spark.sql.udaf.CollectBloomFilter")
# 使用：
spark.sql("select collect_bf(row_id) from tmp group by brand").show()
```

