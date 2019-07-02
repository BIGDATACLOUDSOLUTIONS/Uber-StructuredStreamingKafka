import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object sink {

  def console(df: DataFrame, checkpointLocation: String) = {
    val consoleOutput = df.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("checkpointLocation", checkpointLocation)
      .start()
    consoleOutput.awaitTermination()
  }

  def hdfsSink(df: DataFrame,hdfsPath:String, checkpointLocation: String) = {
    val consoleOutput = df.writeStream
      .outputMode("append")
      .format("csv")
      .option("path", hdfsPath)
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("checkpointLocation", checkpointLocation)
      .start()
    consoleOutput.awaitTermination()
  }

  def kafkaSink(df: DataFrame,topicName:String, checkpointLocation: String) = {
    val newDF = df.select(to_json(struct(df.columns.map(column):_*)).alias("value"))
    //newDF.printSchema()
    val consoleOutput = newDF
      .writeStream
      .format("kafka")
      .outputMode("complete")
      .option("kafka.bootstrap.servers", "192.168.1.105:9092")
      .option("topic", topicName )
      .option("checkpointLocation", checkpointLocation)
      .start()

    consoleOutput.awaitTermination()

  }

}

