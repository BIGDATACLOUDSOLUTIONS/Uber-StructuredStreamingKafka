package org.apache.spark.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryManager

object MyserviceTestShutdownHook extends Logging {
  val install = (streamManager: StreamingQueryManager) => {
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Invoking stop() on all active streaming queries from shutdowm hook")
      try {
        streamManager.active.foreach(query => query.stop())
        streamManager.awaitAnyTermination()
      } catch {
        case e: Throwable =>
          logWarning("Ignoring Exception while stopping streaminh queries", e)
      }
    }
  }


}
