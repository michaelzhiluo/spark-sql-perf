import org.apache.spark.sql.functions._
val resultLocation = "/tmp/tpcds_results"
val result = spark.read.json(resultLocation).filter("timestamp = 1642922868822").select(explode($"results").as("r"))
result.createOrReplaceTempView("result")
spark.sql("select sum(bround((r.parsingTime+r.analysisTime+r.optimizationTime+r.planningTime+r.executionTime)/1000.0,1)) as Runtime_sec  from result").show()
spark.sql("select substring(r.name,1,100) as Name, bround((r.parsingTime+r.analysisTime+r.optimizationTime+r.planningTime+r.executionTime)/1000.0,1) as Runtime_sec  from result").show()
