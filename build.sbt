name := "MyserviceTest"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.0.0"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.0"
// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-core
//libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind
//libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
// https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-scala
//libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.7"



dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"
