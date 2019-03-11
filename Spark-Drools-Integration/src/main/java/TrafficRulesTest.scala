package src.main.java
import scala.collection.JavaConversions._
import org.drools.core.impl.KnowledgeBaseFactory
import org.junit.Assert
import org.junit.Test
import org.kie.api.io.ResourceType
import org.kie.api.runtime.KieSession
import org.kie.api.runtime.rule.RuleContext
import org.kie.internal.builder.KnowledgeBuilderFactory
import org.kie.internal.io.ResourceFactory

class TrafficRulesTest {
	
	val resource= ResourceFactory.newClassPathResource("traffic.drl")
  val kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder()
  kbuilder.add(resource, 
    ResourceType.DRL)
  if (kbuilder.hasErrors()) {
    throw new RuntimeException(kbuilder
      .getErrors().toString())
  }

  val kbase = KnowledgeBaseFactory.newKnowledgeBase()
  kbase.addPackages(kbuilder.getKnowledgePackages())

  @Test
  def testRedInBoston(): Unit = {
    val resp = runTest(Traffic("red", 0))
    Assert.assertEquals("stop", resp.action)
  }
  
  @Test
  def testRedInNewYork(): Unit = {
    val resp = runTest(Traffic("red", 1))
    Assert.assertEquals("stop", resp.action)
  }
    
  @Test
  def testGreenInBoston(): Unit = {
    val resp = runTest(Traffic("green", 0))
    Assert.assertEquals("proceed", resp.action)
  }
  
  @Test
  def testGreenInNewYork(): Unit = {
    val resp = runTest(Traffic("green", 1))
    Assert.assertEquals("proceed", resp.action)
  }

  @Test
  def testYellowInBoston(): Unit = {
    val resp = runTest(Traffic("yellow", 0))
    Assert.assertEquals("accelerate", resp.action)
  }
  
  @Test
  def testYellowInNewYork(): Unit = {
    val resp = runTest(Traffic("yellow", 1))
    Assert.assertEquals("stop", resp.action)
  }
  
  def runTest(traffic: Traffic): TrafficResponse = {
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
  
  def runTest2(light: String, cid: Int): String = {
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

case class Traffic(light: String, cid: Int)
case class DrivingStyle(style: String)
case class TrafficResponse(action: String)

class CityLocator {
  
  def city(traffic: Traffic): String =
    if (traffic.cid == 0) "Boston"
    else "New York"
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