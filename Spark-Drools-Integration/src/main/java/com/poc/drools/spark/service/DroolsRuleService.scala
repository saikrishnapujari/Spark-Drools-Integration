package com.poc.drools.spark.service

import scala.collection.JavaConversions._
import org.drools.core.impl.KnowledgeBaseFactory
import org.kie.api.io.ResourceType
import org.kie.api.runtime.KieSession
import org.kie.api.runtime.rule.RuleContext
import org.kie.internal.builder.KnowledgeBuilderFactory
import org.kie.internal.io.ResourceFactory
import com.poc.drools.spark.util.Utils.{Traffic,DrivingStyle,TrafficResponse,TrafficStats}

object DroolsRuleService {
	
	val resource= ResourceFactory.newClassPathResource("traffic.drl")
  val kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder()
  kbuilder.add(resource,ResourceType.DRL)
  
  if (kbuilder.hasErrors()) {
    throw new RuntimeException(kbuilder.getErrors().toString())
  }
  val kbase = KnowledgeBaseFactory.newKnowledgeBase()
  kbase.addPackages(kbuilder.getKnowledgePackages())

  def runAllRules(traffic: Traffic): TrafficResponse = {
    val session = kbase.newKieSession()
    session.setGlobal("cityLocator", new CityLocator())
    session.insert(traffic)
    session.fireAllRules()
    val trafficResponse = 
        getResults(session, "TrafficResponse") match {
      case Some(x) => x.asInstanceOf[TrafficResponse]
      case None => null
    }
    session.dispose()
    trafficResponse    
  }
  
  def runAllRulesForUDF(light: String, cid: Int): String = {
  	val traffic = Traffic(light,cid)
    val session = kbase.newKieSession()
    session.setGlobal("cityLocator", new CityLocator())
    session.insert(traffic)
    session.fireAllRules()
    val trafficResponse = 
        getResults(session, "TrafficResponse") match {
      case Some(x) => x.asInstanceOf[TrafficResponse]
      case None => null
    }
    session.dispose()
    trafficResponse.action    
  }
  
  def getResults(sess: KieSession,
      className: String): Option[Any] = {
    val fsess = sess.getObjects().filter(o => 
      o.getClass.getName().endsWith(className))
    if (fsess.size > 0) Some(fsess.toList.head)
    else None
  }
}

class CityLocator {
  
  def city(traffic: Traffic): String =
    if (traffic.cid == 0) "Chennai"
    else "Hyderabad"
}

object Functions {
  
  def insertTrafficResponse(kcontext: RuleContext, 
      traffic: Traffic, 
      action: String): Unit = {
    // create and insert a TrafficResponse bean
    // back into the session
    val sess = kcontext.getKieRuntime
      .asInstanceOf[KieSession]
    sess.insert(TrafficResponse(action))
    
    // log the step
    val rulename = kcontext.getRule().getName()
    val cityLocator = sess.getGlobal("cityLocator")
      .asInstanceOf[CityLocator]
    val city = cityLocator.city(traffic)
    Console.println("Rule[%s]: Traffic(%s at %s) => %s"
      .format(rulename, traffic.light, city, action))
  }
  
  def insertDrivingStyle(kcontext: RuleContext, 
      driveStyle: String): Unit = {
    val sess = kcontext.getKieRuntime()
      .asInstanceOf[KieSession]
    Console.println("Driving Style: %s"
      .format(driveStyle))
    sess.insert(DrivingStyle(driveStyle))
  }
}