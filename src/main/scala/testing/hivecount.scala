package testing
import org.apache.spark.sql.SparkSession

object hivecount {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("yarn").appName("java_spark_hive").enableHiveSupport.getOrCreate



    spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS employee (id int, name string, age int, gender string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://aws-logs-723293022411-us-east-1/spark-csv-hive' ")

    spark.sql("SELECT * FROM employee").show()


  }
}

