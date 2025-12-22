package com.sec.eeg.ars.data

import org.json.JSONObject
import org.json4s.DefaultFormats
import org.json4s.ext.EnumSerializer


case class ScenarioResult(hostname: String, process: String, model: String, line: String, diff: Long, scname: String, result: Int, altxt: String, currentstep: Int, snapshot: String, txn: Long, ts: Long)

case class ScenarioDetailsResult(hostname: String, process: String, model: String, line: String, diff: Long, scname: String, result: Int, altxt: String, currentstep: Int, snapshot: String, txn: Long, ts: Long, details: String)

case class ScenarioDetailsResultWithMap(hostname: String, process: String, model: String, line: String, diff: Long, scname: String, result: Int, altxt: String, currentstep: Int, snapshot: String, txn: Long, ts: Long, details: String, params: Map[String,String])

case class ScenarioCommand(hostname: String, scname: String, txn: Long, altxt: String, params: String)

case class AgentEvent(hostname: String, process: String, model: String, scname: String, txn: Long, line:String, status: String)

case class NewDataFromLocal(hostname: String, txn: Long, line:String, altxt: String, code: String)

case class NewDataFromLocalWithParams(hostname: String, txn: Long, line:String, altxt: String, code: String, params: Map[String,String])

case class RequestEmailTrans(hostname:String, ip:String, process:String, line:String, model:String, code:String, subcode:String, snapshot: String)

case class RequestTkinCancel(hostname: String, line:String, txn: Long)

case class RequestReleaseMsg(hostname: String, line:String, txn: Long)

case class RequestServerRestart(hostname: String, line:String, txn: Long)

case class RequestEquipmentDown(hostname: String, line: String, txn: Long, dncode: String)

case class PrePMStart(hostname: String, line: String, chamberid: String, pmcode: String)

case class PrePMComplete(hostname: String, line: String, chamberid: String, pmcode: String)

case class PhysicalPMStart(hostname: String, line: String, chamberid: String, pmcode: String)

case class PhysicalPMComplete(hostname: String, line: String, chamberid: String, pmcode: String)

case class PostPMStart(hostname: String, line: String, chamberid: String, pmcode: String)

case class PostPMComplete(hostname: String, line: String, chamberid: String, pmcode: String)

case class SSBAction(hostname: String, line: String, smlid: String, pmcode: String)

case class ACSCall(hostname: String, line: String, comments: String)

case class LoadPortAutoJob(hostname: String, line: String, comments: String, portLists: List[String], autoflag: String)

case class RecoveryEmailHttpDataFormat(hostname:String, process:String, line:String, model:String, scname:String, title:String, body:String, variables:Map[String,String])

case class EmailHttpDataFormat(hostname:String, ip:String, process:String, line:String, model: String, code:String, subcode:String, variables:Map[String,String])

case class AutoRecoveryDataFormat(process:String, model: String, eqpid:String, line:String, code:String, params:Map[String,String], triggeredBy: String)

object JsonDataFormat {
  implicit val formats: DefaultFormats.type = DefaultFormats
  import org.json4s.JsonDSL._
  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  def toJson(sc: ScenarioCommand) : String = {
    val json =
      ("hostname" -> sc.hostname) ~
        ("scname" -> sc.scname) ~
        ("txn" -> sc.txn) ~
        ("altxt" -> sc.altxt) ~
        ("params" -> sc.params) ~
    compact(render(json))
  }

  def toJson(req: RecoveryEmailHttpDataFormat) : String = {
    val json =
      ("hostname" -> req.hostname) ~
        ("process" -> req.process) ~
        ("line" -> req.line) ~
        ("model" -> req.model) ~
        ("scname" -> req.scname) ~
        ("title" -> req.title) ~
        ("body" -> req.body) ~
        ("variables" -> req.variables) ~
    compact(render(json))
  }

  def toJson(req: EmailHttpDataFormat) : String = {
    val json =
      ("hostname" -> req.hostname) ~
        ("ip" -> req.ip) ~
        ("app" -> "ARS") ~
        ("process" -> req.process) ~
        ("model" -> req.model) ~
        ("line" -> req.line) ~
        ("code" -> req.code) ~
        ("subcode" -> req.subcode) ~
        ("variables" -> req.variables) ~
    compact(render(json))
  }

  def getScenarioProperty_SimpleJSON(json: String) : ScenarioProperty = {
    var ID = ""
    var Owners = scala.collection.mutable.ListBuffer.empty[String]
    var ScenarioTriggerType = eScenarioTriggerType.USER_DEFINED
    var DoNotSendEmailWhenSuccess = false
    var DoNotSendEmailWhenStop = false
    var DoNotSendEmailWhenFail = false
    var StartDelay = 0
    var RetryCnt = 0
    var RetryInterval = 10
    var NextScenarioIfSuccess : NextScenarioCondition = null
    var NextScenarioIfFail : NextScenarioCondition = null
    var IsEnabled = true
    var Priority = 10

    try {
      val jsonObj = new JSONObject(json)

      try{
        ID = jsonObj.get("ID").asInstanceOf[String]
      }catch{
        case _ : Throwable => ID = ""
      }

      try{
        val arry = jsonObj.getJSONArray("Owners")
        for(i <- 0 to arry.length()-1){
          val aaa = arry.optString(i)
          Owners += aaa
        }
      }catch{
        case _ : Throwable => Owners = scala.collection.mutable.ListBuffer.empty[String]
      }

      try{
        val _ScenarioTriggerType = jsonObj.getJSONObject("ScenarioTriggerType").getString("$numberLong").toInt
        ScenarioTriggerType = eScenarioTriggerType.values.find(_.id == _ScenarioTriggerType).getOrElse(eScenarioTriggerType.USER_DEFINED)
      }catch{
        case _ : Throwable =>
          try{
            val _ScenarioTriggerType = jsonObj.get("ScenarioTriggerType").asInstanceOf[Int]
            ScenarioTriggerType = eScenarioTriggerType.values.find(_.id == _ScenarioTriggerType).getOrElse(eScenarioTriggerType.USER_DEFINED)
          }catch{
            case _ : Throwable => ScenarioTriggerType = eScenarioTriggerType.USER_DEFINED
          }
      }

      try{
        DoNotSendEmailWhenSuccess = jsonObj.get("DoNotSendEmailWhenSuccess").asInstanceOf[Boolean]
      }catch{
        case _ : Throwable => DoNotSendEmailWhenSuccess = false
      }

      try{
        DoNotSendEmailWhenStop = jsonObj.get("DoNotSendEmailWhenStop").asInstanceOf[Boolean]
      }catch{
        case _ : Throwable => DoNotSendEmailWhenStop = false
      }

      try{
        DoNotSendEmailWhenFail = jsonObj.get("DoNotSendEmailWhenFail").asInstanceOf[Boolean]
      }catch{
        case _ : Throwable => DoNotSendEmailWhenFail = false
      }

      try{
        StartDelay = jsonObj.getJSONObject("StartDelay").getString("$numberLong").toInt
      }catch{
        case _ : Throwable =>
          try{
            StartDelay = jsonObj.get("StartDelay").asInstanceOf[Int]
          }catch{
            case _ : Throwable => StartDelay = 0
          }
      }

      try{
        RetryCnt = jsonObj.getJSONObject("RetryCnt").getString("$numberLong").toInt
      }catch{
        case _ : Throwable =>
          try{
            RetryCnt = jsonObj.get("RetryCnt").asInstanceOf[Int]
          }catch{
            case _ : Throwable => StartDelay = 0
          }
      }

      try{
        RetryInterval = jsonObj.getJSONObject("RetryInterval").getString("$numberLong").toInt
      }catch{
        case _ : Throwable =>
          try{
            RetryInterval = jsonObj.get("RetryInterval").asInstanceOf[Int]
          }catch{
            case _ : Throwable => StartDelay = 0
          }
      }

      try{
        val _name = jsonObj.get("NextScenarioIfSuccess").asInstanceOf[JSONObject].get("Name").asInstanceOf[String]
        val _text = jsonObj.get("NextScenarioIfSuccess").asInstanceOf[JSONObject].get("Text").asInstanceOf[String]
        NextScenarioIfSuccess = NextScenarioCondition(_name,_text)
      }catch{
        case _ : Throwable => NextScenarioIfSuccess = null
      }

      try{
        val _name = jsonObj.get("NextScenarioIfFail").asInstanceOf[JSONObject].get("Name").asInstanceOf[String]
        val _text = jsonObj.get("NextScenarioIfFail").asInstanceOf[JSONObject].get("Text").asInstanceOf[String]
        NextScenarioIfFail = NextScenarioCondition(_name,_text)
      }catch{
        case _ : Throwable => NextScenarioIfFail = null
      }

      try{
        Priority = jsonObj.getJSONObject("Priority").getString("$numberLong").toInt
        if(Priority <= 0)
          Priority = 10
      }catch{
        case _ : Throwable =>
          try{
            Priority = jsonObj.get("Priority").asInstanceOf[Int]
            if(Priority <= 0)
              Priority = 10
          }catch{
            case _ : Throwable => Priority = 10
          }
      }

      try{
        IsEnabled = jsonObj.get("IsEnabled").asInstanceOf[Boolean]
      }catch{
        case _ : Throwable => IsEnabled = false
      }
    }
    catch{
      case ex : Throwable =>
        println(s"Invalid json: $ex")
    }

    ScenarioProperty(ID,Owners.toList,ScenarioTriggerType,DoNotSendEmailWhenSuccess,DoNotSendEmailWhenStop,DoNotSendEmailWhenFail,StartDelay,RetryCnt,RetryInterval,NextScenarioIfSuccess,NextScenarioIfFail,IsEnabled,Priority)
  }

  def getScenario(json: String) : Scenario = {
    implicit val formats : Formats = DefaultFormats + new EnumSerializer(eScenarioTriggering) + new EnumSerializer(eScenarioOperation) + new EnumSerializer(eScenarioScript) + new ScenarioStepSerializer
    val sc = parse(json)
    sc.extract[Scenario]
  }
}