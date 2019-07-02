import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

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
      //  .option("kafka.bootstrap.servers", "PLAINTEXT://ip-172-31-38-146.ec2.internal:6667")
      .option("kafka.bootstrap.servers", "192.168.1.105:9092")
      .option("subscribe", "payment")
      .option("startingOffsets", "earliest")
      //.option("includeTimestamp",true)
      .load.selectExpr("topic", "CAST(value AS STRING)").select($"value")

    val schema = (new StructType)
      .add("id", "int", true)
      .add("event_date", "timestamp", false)
      .add("tour_value", "double", true)
      .add("id_driver", "int", true)
      .add("id_passenger", "int", true)

    val rdf = df.select(from_json($"value", schema) as "value").select("value.*")
    rdf.printSchema()

    // val agg = rdf.groupBy(rdf.col("id_driver")).agg(sum("tour_value"))

    rdf.createOrReplaceTempView("uber")

    val df2 = spark.sql("select id_driver, sum(tour_value) as totalSum from uber group by id_driver")


    val windowedCounts = rdf
      .withWatermark("event_date", "1 minutes")
      .groupBy(
        window($"event_date", "1 minutes"),
        $"id_driver")
      .agg(sum("tour_value") as "totalSum")

    val filteredDF = windowedCounts.select("id_driver", "totalSum").filter("totalSum > 200")

    windowedCounts.printSchema()

    //  val windowedCounts = rdf
    // .groupBy("id_driver").count()
    sink.hdfsSink(rdf,"C:\\Users\\RAJESH\\Desktop\\data","C:\\Users\\RAJESH\\Desktop\\checkpointLocations\\checkpointLocation")
    sink.kafkaSink(df2, "paymentSink1", "C:\\Users\\RAJESH\\Desktop\\checkpointLocations\\checkpointLocation1")
    sink.kafkaSink(filteredDF, "paymentSink", "C:\\Users\\RAJESH\\Desktop\\checkpointLocations\\checkpointLocation2")
  }
}
