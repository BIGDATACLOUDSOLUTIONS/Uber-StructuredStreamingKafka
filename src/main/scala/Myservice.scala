import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.util.MyserviceTestShutdownHook

object Myservice {
  def main(args: Array[String]): Unit = {
    // Logger.getLogger("org").setLevel(Level.ERROR)
    // val logger = Logger.getLogger(getClass.getName)
    val logger = LogManager.getRootLogger
    logger.setLevel(Level.ERROR)
    logger.info("starting myservice...")

    val spark = SparkSession
      .builder()
      .appName("myservice")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.105:9092")
      .option("subscribe", "payment,paymentSink")
      .option("startingOffsets", "earliest")
      .load.selectExpr("topic", "CAST(value AS STRING)")

    val sourceDF = df.filter($"topic"==="payment").select($"value")
    val partialAggDF = df.filter($"topic"==="paymentSink").select($"value")


    val schema = (new StructType)
      .add("id", "int", true)
      .add("event_date", "timestamp", false)
      .add("tour_value", "double", true)
      .add("id_driver", "int", true)
      .add("id_passenger", "int", true)

    val flattenedDF = sourceDF.select(from_json($"value", schema) as "value").select("value.*")
    flattenedDF.printSchema()
    flattenedDF.createOrReplaceTempView("uber")
    val useCase2  = spark.sql("select id_driver, sum(tour_value) as totalSum from uber group by id_driver")


       val windowedCounts = flattenedDF
          .withWatermark("event_date", "1 minutes")
          .groupBy(
            window($"event_date", "1 minutes"),
            $"id_driver")
          .agg(sum("tour_value") as "totalSum")

        val filteredDF = windowedCounts.select("id_driver", "totalSum").filter("totalSum > 20")
        filteredDF.printSchema()

    sink.hdfsSink(sourceDF,"C:\\Users\\RAJESH\\Desktop\\data\\data1","C:\\Users\\RAJESH\\Desktop\\checkpointLocations\\checkpointLocation1")
    sink.kafkaSink(useCase2, "paymentSink", "C:\\Users\\RAJESH\\Desktop\\checkpointLocations\\checkpointLocation3")
    sink.kafkaSink(filteredDF, "paymentSink1", "C:\\Users\\RAJESH\\Desktop\\checkpointLocations\\checkpointLocation4")
    sink.hdfsSink(partialAggDF,"C:\\Users\\RAJESH\\Desktop\\data\\data2","C:\\Users\\RAJESH\\Desktop\\checkpointLocations\\checkpointLocation2")

    MyserviceTestShutdownHook.install(spark.streams)
    spark.streams.awaitAnyTermination()
  }
}