package com.poc.drools.spark.util

object Utils {
  
	case class TrafficStatsOp(cid:Integer,light:String,style:String,action:String)
	case class TrafficStats(traffic: Traffic, style: DrivingStyle, response: TrafficResponse)
	case class Traffic(light: String, cid: Int)
	case class DrivingStyle(style: String)
	case class TrafficResponse(action: String)
	
}