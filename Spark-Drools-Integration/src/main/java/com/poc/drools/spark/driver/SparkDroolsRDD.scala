package com.poc.drools.spark.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.poc.drools.spark.util.Utils
import com.poc.drools.spark.service.DroolsRuleService
import com.poc.drools.spark.util.CustomStructUtils
import com.poc.drools.spark.util.Utils.TrafficStatsOp
import org.apache.spark.sql.Row

object SparkDroolsRDD {
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
  	
  	import spark.implicits._
  	/**
  	 * Apply Drools rules by converting to RDD then applying rules function using map()
  	 */
  	val rddDrools = df_Drools.rdd.map[Row]{row => 
  		val ts = CustomStructUtils.prepTrafficStatsObj(row);
  		val tr = DroolsRuleService.runAllRules(ts.traffic)
  		Row(ts.traffic.cid,ts.traffic.light,ts.style.style,tr.action)
  		}
  	
  	val df_Drools_Applied = spark.createDataFrame(rddDrools, CustomStructUtils.prepTrafficStatsStructType)
  	df_Drools_Applied.show();
  	println(df_Drools_Applied.schema)
	}
}