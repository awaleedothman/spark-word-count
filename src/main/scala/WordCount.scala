import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Word Count").getOrCreate()
    val sc = spark.sparkContext
    val lines = sc.textFile("input")
    val counts = lines
      .flatMap(line => line.split(" "))
      .map(word => (word.replaceAll("[^a-zA-Z ]", "").toLowerCase(), 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("output")
    spark.stop()
  }

}
