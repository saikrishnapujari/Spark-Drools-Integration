package com.poc.drools.spark.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import com.poc.drools.spark.service.DroolsRuleService
import scala.reflect.api.materializeTypeTag

object SparkDroolsDataframe {
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
  	 * Apply Drools rules using - UDF
  	 */
  	val df_Drools_Applied = df_Drools.withColumn("response", runAllRulesUDF(df_Drools("traffic_light"),lit(0)))
  	df_Drools_Applied.show();
  	println(df_Drools_Applied.schema)
  	
  }
  /**
   * UDF - that takes 2 arguments : Traffic light color and City-ID
   */
  def runAllRulesUDF = udf(DroolsRuleService.runAllRulesForUDF(_:String,_:Integer))
}