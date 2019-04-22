package com.poc.drools.spark.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.poc.drools.spark.util.Utils.{TrafficStats}
import com.poc.drools.spark.util.Utils
import com.poc.drools.spark.service.DroolsRuleService
import com.poc.drools.spark.service.DroolsRuleService
import com.poc.drools.spark.util.CustomStructUtils
import com.poc.drools.spark.util.Utils.TrafficStatsOp

object SparkDroolsDataset {
  def main(args:Array[String]){
  	/**
  	 * To run in Windows - Eclipse
  	 * System property for hadoop.home.dir - point it to winutils.exe dir
  	 */
  	sys.props.+=(("hadoop.home.dir","C:\\hadoop_home"))
  	
  	/**
  	 * Spark session creation
  	 */
  	val sparkConf = new SparkConf().setMaster("local").setAppName("Sample")
  	val sparkContext = new SparkContext(sparkConf)
  	val spark = SparkSession.builder().appName("Sample").getOrCreate()
  	
  	/**
  	 * Read input data
  	 */
  	val df_Drools = spark.read.option("header", "true").csv(".\\src\\main\\resources\\Sample.csv")
  	df_Drools.show();
  	println(df_Drools.schema)
  	
  	/**
  	 * Apply Drools rules to data-set of type TrafficStats
  	 */
  	import org.apache.spark.sql.Encoders
  	//Encoders for input and output
		val tafficStatsEncoder = Encoders.product[Utils.TrafficStats]
  	val trafficStatsOpEncoder = Encoders.product[Utils.TrafficStatsOp]
  	val dsTrafficStats = CustomStructUtils.prepTrafficStatsStruct(df_Drools).as(tafficStatsEncoder)
  	val dsRulesApplied = dsTrafficStats.map{ts => 
  		val tr = DroolsRuleService.runAllRules(ts.traffic)
  		TrafficStatsOp(ts.traffic.cid,ts.traffic.light,ts.style.style,tr.action)
  	}(trafficStatsOpEncoder)
  	
  	val df_Drools_Applied = dsRulesApplied.toDF()
  	df_Drools_Applied.show();
  	println(df_Drools_Applied.schema)
  	
  }
}