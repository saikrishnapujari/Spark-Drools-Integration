import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import src.main.java.TrafficRulesTest

object SparkDroolsIntTest {
  def main(args:Array[String]){
  	sys.props.+=(("hadoop.home.dir","C:\\hadoop_home"))
  	
  	val sparkConf = new SparkConf().setMaster("local").setAppName("Sample")
  	val sparkContext = new SparkContext(sparkConf)
  	val spark = SparkSession.builder().appName("Sample").getOrCreate()
  	
  	/*
  	 * Expected output json sample 
  	 */
  	val df_Drools = spark.read.option("header", "true").csv(".\\src\\main\\resources\\Sample.csv")
  	df_Drools.show();
  	println(df_Drools.schema)
  	val df_Drools_Applied = df_Drools.withColumn("response", testFun(df_Drools("traffic_light"),lit(0)))
  	df_Drools_Applied.show();
  	println(df_Drools_Applied.schema)
  	
  }
  def testFun = udf(new TrafficRulesTest().runTest2(_:String,_:Integer))
}