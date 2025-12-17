import com.sec.eeg.ars.actor

import akka.actor.Actor
import akka.pattern.ask
import akka.util.Timeout
import com.lambdaworks.redis.RedisClient
import com.mongodb.BasicDBObject
import com.mongodb.client.model.UpdateOptions
import com.sec.eeg.ars.data.ServiceConfig.database
import com.sec.eeg.ars.data._

import scala.collection.JavaConverters._
import com.sec.eeg.ars.remote.{AgentStatus, Function, GRPCConnector, HttpConnector}
import com.typesafe.config.Config
import org.bson.{BsonDocument, Document}
import org.joda.time.format.ISODateTimeFormat

import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


case class JobActorException(message: String, cause: Throwable = None.orNull) extends Exception(message, cause)

case class AutoRecoveryData(process: String, model: String, eqpid: String, line: String, ears_code: String, txn: Long, data: String, triggeredBy: String)

case class AutoRecoveryBulkData(body: String)

case class AutoRecoveryData_Scheduled(process: String, model: String, eqpid: String, line: String, txn: Long, modified_time: Long, scname: String)

class JobActor(conf: Config) extends Actor {
  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val timeout: Timeout = Timeout(5 seconds)

  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  import context.dispatcher //RedisHGetData null 처리
  private val log = LoggerFactory.getLogger(classOf[JobActor])


  val redisClient = {
    if(ServiceConfig.RedisSentinelConnection.exist(ch => ch == "#"))
      RedisClient.create(s"redis-sentinel://visuallove@${ServiceConfig.RedisSentinelConnection}")
    else
      RedisClient.create(s"redis://visuallove@${ServiceConfig.RedisSentinelConnection}")
  }

  override preStart(): Unit = {
    log.info("JobActor preStart")
  }

  override postStop(): Unit = {
    log.info("JobActor postStop")
  }


  def GetAgentMetaInfo(process: String, model: String, eqpid: String) : Array[String] = {
    var agentMetaInfo = ""
    val redisConnection = redisClient.connect()
    try{
      agentMetaInfo = redisConnection.sync().hget(s"AgentMetaInfo:${process}-${model}", eqpid)
    }finally {
      redisConnection.close()
    }
    if(agentMetaInfo == "") {
      log.info(s"get AgenMetaInfo failed: ${process}, ${model}, ${eqpid}, not found meta info")
      Array.empty[String]
    } else {
      agentMetaInfo.split(":")
    }
  }

  //get ip, local ip
  def getIpLocalIp(eqpid: String) : (String, String) = {
    var ip_ipLocal : (String, String) = null

    val eqpinfo_table = ServiceConfig.database.getCollection("EQP_INFO")
    val search = new BasicDBObject().append("eqpId",eqpid)
    val projection = new BasicDBObject().append("ipAddr", true).append("ipAddrL",true).append("_id", false)
    val searchRet = eqpinfo_table.find(search).projection(projection).asScala
    if(searchRet.size == 1){
      ip_ipLocal = (searchRet.head.getString("ipAddr"),searchRet.head.getOrDefault("ipAddrL","_").toString)
    }
    ip_ipLocal
  }


  def RunAction(func:Fucntion.Value, process:String, model:String, eqpid:String, jsonString:String) : AgentStatus.Value = {
    var results : AgentStatus.Value = AgentStatus.CommandFailed
    val getIpIpLocal = getIpIpLocal(eqpid)
    var ip = ""
    var ipLocal = ""
    if(getIpIpLocal != null){
      ip = getIpIpLocal._1
      ipLocal = getIpIpLocal._2
    }
    val agentMetaInfo = GetAgentMetaInfo(process, model, eqpid)
    if(!agentMetaInfo.empty) {
      val agentCommandType = if(agentMetaInfo.size == 5) agentMetaInfo(4) else "grpc"
      if(agentCommandType == "grpc")
        results = GRPCConnector.sendScenarioCommandToAgent(process,model,eqpid,jsonString)
      else
        results = HttpConnector.sendCommandToEquip(func, process, model, eqpid, jsonString)
    }
    results
  }

  def getMap(content: String) : Map[String, String] = {
    val ret = scala.collection.mutable.ListMap.empty[String,String]
    val items = content.split(',')
    items.foreach(item => {
      val index = item.indexOf(':')
      if(index > 0){
        val _key = item.substring(0,index)
        val _value = item.substring(index+1)
        if(_key != "" && _value != ""){
          val value = _value.replace("'","") // remove '
          ret.+=((_key,value.trim))
        }
      }
    })
    return ret.toMap
  }

  def getSCProperty(process: String, model: String, scname: String) : (ScenarioProperty, Int) = {
    var property: ScenarioProperty = null
    var category: Long = -1
    try{
      val property_table = ServiceConfig.database.getCollection("SC_PROPERTY", classOf[BsonDocument])
      val search = new BasicDBObject().append("process",process).append("eqpModel",model).append("scname",scname)
      val searchCursor = property_table.find(search)
      val propCols = searchCursor.asScala

      if (propCols.size == 1) {
        val _property = propCols.head.getDocument("property").toJson()
        property = JsonDataFormat.getScenarioProperty_SimpleJSON(_property)
        category = propCols.head.getInt64("scCategory").getValue
      }
      else {
        log.warn(s"propCols.size is not 1 : ${propCols.size.toString}")
      }
      
      (property, category.toInt)
    }
    catch {
      case ex: Throwable => log.error(s"Excpetion : $process, $model, $scname")
      (null, -1)
    }
  }

  def updateRecoveryStatus(eqpid: String, process: String, model: String, scname: String, txn: Long, line: String, status: String): Any = {
    try{
      val recovery_table = ServiceConfig.database.getCollection("EQP_AUTO_RECOVERY")
      val prop = getSCProperty(process, model, scname)

      if(prop._1 == null){
        throw JobActorException(s"Scenario property is null: $process, $model, $scname")
      }

      val _update = new Document.append("status", status)
      //append("ears_code", scname) 추가. eqpid&txn_seq 중복 발생 가능으로 unique key 추가
      recovery_table.updateOne(new Document().append("eqpid", eqpid).append("txn_seq", txn).append("ears_code", scname),
        new Document().append("$set", _update), new UpdateOptions().upsert(true))

      context.actorSelection("/user/Master/MessageProducer") ! SendMessageToStatusTopic(process, model, eqpid, scname, txn, status, Map.empty[String, Any])
    }
    catch{
      case ex: Throwable => log.error(s"Exception : $ex")
    }
  }
    
  
  def checkRetryForRecovery(scr: ScenarioDetailsResultWithMap, property: ScenarioProperty): (Boolean, Boolean) = {
    var bRetry = false

    if (scr.result == 0 || scr.result == 1)
      return (true, false)

    val recovery_table = ServiceConfig.database.getCollection("EQP_AUTO_RECOVERY")
    val search = new BasicDBObject().append("eqpid", eqpid).append("txn_seq", txn).append("ears_code", scname)
    val searchRet recovery_table.find(search).asScala
    if (searchRet.size == 1) {
      val _retryinfo = searchRet.head.getString("retry").split("/")
      if (_retryinfo.length == 3) { // current/total/interval
        if (_retryinfo(0).toInt < _retryinfo(1).toInt) {
          val update_retry = s"${_retryinfo(0).toInt + 1}/${_retryinfo(1)}/${_retryinfo(2)}"

          val _update = new Document().append("retry", update_retry)
          recovery_table.updateOne(new Document().append("eqpid", scr.hostname).append("txn_seq", scr.txn),
            new Document().append("$set", _update), new UpdateOptions().upsert(true))
          log.info(s"Realtime Recovery retry setting: ${scr.hostname}, ${scr.scname}, ${scr.txn}, cnt-${_retryinfo(0).toInt + 1}")

          val modified_time = System.currentTimeMillis() + _retryinfo(2).toLong * 1000
          context.actorSelection("/user/Master/RedisActor") ! RedisSetExData(s"deferred@${scr.process}@${scr.model}@${scr.hostname}@${scr.line}@${scr.txn}@$modified_time@${scr.scname}", "1", _retryinfo(2).toLong)
          bRetry = true // retry check done and set retry
        }
        return (true, bRetry)
      }
    } else {
      return (false, false)
    }

    (true, bRetry)
  }

  def sendAutorecoveryEmail(mailTitle: String, scbody: Scenario, scr: ScenarioDetailsResultWithMap): Unit = {
    def htmlEscape(str:String): String = { //HTML 사용 escape 문자 변경
      str.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll("\"","&quot;").replaceAll("'","&#39;")
    }

    log.info(s"Send ScenarioResult: $mailTitle")
    var mailContents = ""
    mailContents +=
      s"""
         | <h3>[${MultiLangMgr.autorecovery_information}]</h3>
         | <ul style="list-style-type:disc">
      """.stripMargin

    mailContents += s"""<li><font size="3">${MultiLangMgr.eqpid}</font> : <font size="3" color="blue">${scr.hostname}</font></li>"""
    mailContents += s"""<li><font size="3">${MultiLangMgr.scname}</font> : <font size="3" color="blue">${scr.scname}</font></li>"""
    mailContents += s"""<li><font size="3">${MultiLangMgr.scdesc}</font> : <font size="3" color="blue">${htmlEscape(scr.altxt)}</font></li>"""

    var _triggerInfo = "Unknown"
    if (scr.params.contains("@Trigger")) {
      _triggerInfo = scr.params("@Trigger")
    }

    mailContents += s"""<li><font size="3">${MultiLangMgr.trigger}</font> : <font size="3" color="blue">${_triggerInfo}</font></li>"""

    if (scr.details != "") {
      val _details = htmlEscape(scr.details).replaceAll("@@", "<br>")

      _triggerInfo match {
        case "Log" =>
          mailContents += s"""<li><font size="3">${MultiLangMgr.matched_log}</font> : <font size="3" color="blue">${_details}</font></li>"""
        case "Scheduler" =>
          mailContents += s"""<li><font size="3">${MultiLangMgr.cron_expression}</font> : <font size="3" color="blue">${_details}</font></li>"""
        case "Status" =>
          mailContents += s"""<li><font size="3">${MultiLangMgr.status_info}</font> : <font size="3" color="blue">${_details}</font></li>"""
        case "SE" =>
          var userInfo = ""
          if (scr.params.contains("@UserInfo")) {
            userInfo = scr.params("@UserInfo")
          }
          mailContents += s"""<li><font size="3">${MultiLangMgr.user_info}</font> : <font size="3" color="blue">$userInfo</font></li>"""
        case "Scenario" =>
          var prev_scenario = ""
          if (scr.params.contains("@PrevScenario")) {
            prev_scenario = scr.params("@PrevScenario")
          }
          mailContents += s"""<li><font size="3">${MultiLangMgr.prev_scenario}</font> : <font size="3" color="blue">$prev_scenario</font></li>"""
        case _ =>
          log.info(s"triggerInfo is not valid: ${_triggerInfo}")
      }
    }
    mailContents += s"""<li><font size="3">${MultiLangMgr.timespent}</font> : <font size="3" color="blue">${(scr.diff / 6000).toInt}${MultiLangMgr.min} ${(scr.diff / 1000 % 60).toInt}${MultiLangMgr.sec}</font></li>"""

    if (scr.params.nonEmpty) {
      scr.params.filterNot(p => p._1 == "ALTXT" || p._1 == "ALID").foreach(p => {
        mailContents += s"""<li><font size="3">${p._1}</font> : <font size="3" color="blue">${htmlEscape(p._2)}</font></li>"""
      })
    }

    mailContents += "</ul><br>"


    mailContents += s"<h3>[${MultiLangMgr.scdetails}]</h3>"

    for (index <- 0 to scbody.Step.length - 1) {
      if (index == (scr.currentstep - 1) && index < scbody.Step.length - 1) {
        if (scr.result == 2 || scr.result == 3 || scr.result == 6)
          mailContents += s"""<strong><font color="red">${index + 1}. ${htmlEscape(scbody.Step(index).DescriptionToEmail)} </font></strong><br>"""
        else
          mailContents += s"""<font color="blue">${index + 1}. ${htmlEscape(scbody.Step(index).DescriptionToEmail)} </font><br>"""
      }
      else {
        if (index > (scr.currentstep - 1))
          mailContents += s"""<strike><font color="gray">${index + 1}. ${htmlEscape(scbody.Step(index).DescriptionToEmail)}</font></strike><br>"""
        else
          mailContents += s"""${index + 1}. ${htmlEscape(scbody.Step(index).DescriptionToEmail)}<br>"""
      }
    }
    mailContents += s"""<P><A href="http://${ServiceConfig.HttpSrvPublicUrl}/ARS/History/${scr.hostname}/${scr.txn}" target="_blank">${MultiLangMgr.view_history}</A></P><br>"""


    val reqMsg = RecoveryEmailHttpDataFormat(scr.hostname, scr.process, scr.line, scr.model, scr.scname, mailTitle, mailContents, Map("__snapshot__" -> scr.snapshot))
    val rcv = HttpConnector.SendRecoveryEmail(reqMsg)
    if(rcv._1) {
      log.info(s"Sending Recovery Email is done: ${reqMsg.title}")
    }
    else {
      log.warn(s"Sending Recovery Email failed: ${reqMsg.title}, ${rcv._2}")
    }
  }

  def CheckNextScenarioForRecovery(scr: ScenarioDetailsResultWithMap, property: ScenarioProperty, params: Map[String, String]): Unit = {
    if(scr.result != 0 && scr.result != 1) //자동조치 실패 시 시나리오 실행 여부 (시나리오 중지는 제외)
    {
      if(property.NextScenarioIfFail != null) { //실패시 시나리오가 존재하면

        val txn = System.currentTimeMillis()
        val modifiedParams = scala.collection.mutable.Map.empty[String, String]
        params.map(modifiedParams.+=(_))
        modifiedParams("@Trigger") = "Scenario"
        modifiedParams("PrevScenario") = s"${scr.scname}"
        //val jsonParams = Json(DefaultFormats).write(modifiedParams)

        SaveAutoRecovery(scr.process, scr.model, scr.hostname, scr.line, property.NextScenarioIfFail.Name, txn, modifiedParams.toMap, "Scenario", property.NextScenarioIfFail.Name)
        log.info(s"Recovery failed and new scenario inserted: ${scr.hostname},${property.NextScenarioIfFail.Name},$txn")
      }
    }
    else if (scr.result == 0) { //자동조치 성공 시 시나리오 실행 여부
      if(property.NextScenarioIfSuccess != null) { //성공시 시나리오가 존재하면
        val txn = System.currentTimeMillis()
        val modifiedParams = scala.collection.mutable.Map.empty[String, String]
        params.map(modifiedParams.+=(_))
        modifiedParams("@Trigger") = "Scenario"
        modifiedParams("PrevScenario") = s"${scr.scname}"
        //val jsonParams = Json(DefaultFormats).write(modifiedParams)

        SaveAutoRecovery(scr.process, scr.model, scr.hostname, scr.line, property.NextScenarioIfSuccess.Name, txn, modifiedParams.toMap, "Scenario", property.NextScenarioIfSuccess.Name)
        log.info(s"Recovery succeeded and new scenario inserted: ${scr.hostname},${property.NextScenarioIfSuccess.Name},$txn")
      }
    }
  }

  override def receive: Receive = {
    case AgentEvent(eqpid, proc, model, scname, txn, line, status) =>
      if (status != null && status != "") {
        updateRecoveryStatus(eqpid, proc, model, scname, txn, line, status)
      }
      else {
        log.warn("Invalid AgentEvent")
      }
    case RequestEmailTrans(hostname, ip, process, line, model, code, subcode, snapshot) =>
      val reqMsg = EmailHttpDataFormat(hostname, ip, process, line, model, code, subcode, Map("__snapshot__" -> snapshot))
      val ret = HttpConnector.SendEmailNotification(reqMsg)
      if (ret._1) {
        log.info(s"Sending RequestEmailTrans is done: ${reqMsg.code}-${reqMsg.subcode}")
      }
      else {
        log.info(s"Sending RequestEmailTrans failed: ${reqMsg.code}-${reqMsg.subcode}, ${ret._2}")
      }
    case scr: ScenarioDetailsResultWithMap =>
      try{
        var scResult = ""
        if (scr.result == 7) { //skip
          updateRecoveryStatus(scr.hostname, scr.process, scr.model, scr.scname, scr.txn, scr.line, "Skip")
          scResult = "Skip"
          log.info("Scenario skip!!")
        }
        else {
          val property = getSCProperty(scr.process, scr.model, scr.scname)
          if (property._1 == null) {
            throw JobActorException(s"Scenario property is null: ${scr.process}, ${scr.model}, ${scr.scname}")
          } else {
            log.info(s"ScenarioResult - process: ${scr.process}, line: ${scr.line}, model: ${scr.model}, eqpid: ${scr.hostname}, scname: ${scr.scname}, diff: ${scr.diff}, result: ${scr.result}, category:${property._2}")
            
            var retry: (Boolean, Boolean) = null
            retry = checkRetryForRecovery(scr, property._1)

            if(!retry._1) {
              log.error(s"Exception occurs - process: ${scr.process}, line: ${scr.line}, model: ${scr.model}, eqpid: ${scr.hostname}, scname: ${scr.scname}, diff: ${scr.diff}, result: ${scr.result}")
            }
            else if (retry._2) { //must retry
              updateRecoveryStatus(scr.hostname, scr.process, scr.model, scr.scname, scr.txn, scr.line, "Retry")
              scResult = "Retry"
              log.info(s"Retry Scenario - process: ${scr.process}, line: ${scr.line}, model: ${scr.model}, eqpid: ${scr.hostname}, scname: ${scr.scname}, diff: ${scr.diff}, result: ${scr.result}")
            }
            else {
              val _sc = context.actorSelection("/user/Master/RedisActor") ? RedisHGetData(s"${scr.process}-${scr.model}", s"${scr.name}.${scr.ts}")
              _sc.map { //RedisHGetData null 예외처리
                case Some(scbody: Scenario) =>
                  //_sc 를 이미 future 로 처리했으므로, Await, Scenario로 casting 불필요
                  var bPass = false
                  var mailTitle = "[EARS]"

                  scr.result match {
                    case 0 => //success
                      mailTitle += s"[${MultiLangMgr.success}][${scr.hostname}] ${scbody.Description}"
                      updateRecoveryStatus(scr.hostname, scr.process, scr.model, scr.scname, scr.txn, scr.line, "Success")
                      scResult = "Success"
                    case 1 => //stop
                      mailTitle += s"[${MultiLangMgr.stop}][${scr.hostname}] ${scbody.Description}"
                      updateRecoveryStatus(scr.hostname, scr.process, scr.model, scr.scname, scr.txn, scr.line, "Stopped")
                      scResult = "Stopped"
                    case 2 => //fail
                      mailTitle += s"[${MultiLangMgr.fail} - ${scr.currentstep}][${scr.hostname}] ${scbody.Description}"
                      updateRecoveryStatus(scr.hostname, scr.process, scr.model, scr.scname, scr.txn, scr.line, "Failed")
                      scResult = "Failed"
                    case 3 => //script fail
                      mailTitle += s"[${MultiLangMgr.scriptfail}][${scr.hostname}] ${scbody.Description}"
                      updateRecoveryStatus(scr.hostname, scr.process, scr.model, scr.scname, scr.txn, scr.line, "ScriptFailed")
                      scResult = "ScriptFailed"
                      case 6 => //vision delay
                      mailTitle += s"[${MultiLangMgr.visiondelay}][${scr.hostname}] ${scbody.Description}"
                      updateRecoveryStatus(scr.hostname, scr.process, scr.model, scr.scname, scr.txn, scr.line, "VisionDelayed")
                      scResult = "VisionDelayed"
                    case r: Int =>
                      updateRecoveryStatus(scr.hostname, scr.process, scr.model, scr.scname, scr.txn, scr.line, "Unknown")
                      scResult = "Unknown"
                      log.error("Invalid Scenario Result Received")
                    case _ =>
                      updateRecoveryStatus(scr.hostname, scr.process, scr.model, scr.scname, scr.txn, scr.line, "Unknown")
                      scResult = "etc"
                      log.error("Invalid Scenario Result Received: It is not int type")
                  }
                

                  if (scr.result == 0 && property._1.DoNotSendEmailWhenSuccess) {
                    bPass = true
                    log.info("Auto recovery success, but result mail will not be sent")
                  } else if (scr.result == 1 && property._1.DoNotSendEmailWhenStop) {
                    bPass = true
                    log.info("Auto recovery stopped, but result mail will not be sent")
                  } else if (scr.result != 0 && scr.result != 1 && property._1.DoNotSendEmailWhenFail) {
                    bPass = true
                    log.info(s"Auto recovery failed, but result mail will not be sent")
                  }

                  if (!bPass)
                    sendAutorecoveryEmail(mailTitle, scbody, scr)
                  
                  CheckNextScenarioForRecovery(scr, property._1, scr.params)

                case None =>
                  log.warn(s"RedisHGetData None fail - ${scr.process}, line: ${scr.line}, model: ${scr.model}, eqpid: ${scr.hostname}, scname: ${scr.scname}, diff: ${scr.diff}, result: ${scr.result}")
                  scResult = "DBFail"
                case x =>                
                  log.warn(s"RedisHGetData x fail - ${scr.process}, line: ${scr.line}, model: ${scr.model}, eqpid: ${scr.hostname}, scname: ${scr.scname}, diff: ${scr.diff}, result: ${scr.result}")
                  scResult = "Unknown"
              }
            }
          }
        }
        context.actorSelection("/user/Master/MessageProducer") ! SendMessageToStatusTopic(scr.process, scr.model, scr.hostname, scr.scname, scr.txn, scResult, scr.params)
      } catch {
        case ex: Throwable => log.warn(s"Scenario Result failed: ${ex.getMessage}")
      }

    case AutoRecoveryData_Scheduled(process, model, eqpid, line, txn, modified_time, scname) =>
      try{
        var search: BasicDBObject = null
        if (scname == ""){
          search = new BasicDBObject().append("eqpid", eqpid).append("txn_seq", txn)
        } else {
          search = new BasicDBObject().append("eqpid", eqpid).append("txn_seq", txn).append("ears_code", scname)
        }
        val collection = ServiceConfig.database.getCollection("EQP_AUTO_RECOVERY")
        val searchRet = collection.find(search).asScala
        if (searchRet.size == 1) {
          val code = searchRet.head.getString("ears_code")
          val prop = getSCProperty(process, model, code)
          if (prop._1 == null) {
            throw JobActorException(s"Scenario property is null: $process, $model, $code")
          }

          if (prop._1.IsEnabled) {
            val curTime = System.currentTimeMillis()
            log.info(s"System time: $curTime, modified_time: $modified_time")
            if (Math.abs(curTime - modified_time) > ServiceConfig.RecoveryAllowableTimeLimit.toMillis) {
              log.warn(s"The allowable time limit is so big: system-$curTime, modified_time-$modified_time")
              throw JobActorException(s"RecoveryAllowableTimeLimit($curTime,$modified_time): $process, $model, $eqpid, $line, $txn")
            }

            val jsonParams = searchRet.head.get("params").asInstanceOf[Document].toJson
            val json_contents = JsonDataFormat.toJson(ScenarioCommand(eqpid, code, txn, code, jsonParams))
            SendRecovery(eqpid, process, model, code, txn, line, json_contents)
          } else {
            log.info(s"Scenario is disabled: $process, $model, $code")
          }
        } else {
          throw JobActorException(s"Invalid recovery data: $process, $model, $eqpid, $line, $txn, $scname, ret: ${searchRet.size}")
        }
      } catch {
        case ex: Throwable => log.warn(s"AutoRecoveryData_Scheduled exception: $process, $model, $eqpid, $line, $txn, $scname, ${ex.getMessage}, ${ex.getStackTraceString}")
      }
    
    case NewDataFromLocal(hostname, txn, line, altxt, code) =>
      try{
        val curTS = System.currentTimeMillis()
        val proc_model = getProcessModel(hostname)
        val triggeredBy = "Unknown"
        if(proc_model != null)
          SaveAutoRecovery(proc_model._1, proc_model._2, hostname, line, code, curTS, Map.empty[String, String], triggeredBy, altxt)
        else
          log.warn(s"There is no process, model info with $hostname, $line")
      } catch {
        case ex: Throwable => log.warn(s"NewDataFromLocal exception: $hostname, $line, $code, ${ex.getMessage}, ${ex.getStackTraceString}")
      }

    case NewDataFromLocalWithParams(hostname, txn, line, altxt, code, params) =>
      try{
        val curTS = System.currentTimeMillis()
        val proc_model = getProcessModel(hostname)
        val triggeredBy = if (params.contains("@Trigger")) params("@Trigger") else "Unknown"
        if(proc_model != null)
          SaveAutoRecovery(proc_model._1, proc_model._2, hostname, line, code, curTS, params, triggeredBy, altxt)
        else
          log.warn(s"There is no process, model info with $hostname, $line")
      } catch {
        case ex: Throwable => log.warn(s"NewDataFromLocalWithParams exception: $hostname, $line, $code, ${ex.getMessage}, ${ex.getStackTraceString}")
      }

    case AutoRecoveryData(process, model, eqpid, line, code, txn, params, triggeredBy) =>
      try{
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
        val _params = parse(params).extract[Map[String, String]]
        SaveAutoRecovery(process, model, eqpid, line, code, txn, _params, triggeredBy, code)
      } catch {
        case ex: Throwable => log.warn(s"AutoRecoveryData exception: $eqpid, $line, $code, ${ex.getMessage}, ${ex.getStackTraceString}")
      }

    case AutoRecoveryBulkData(jsonString) =>
      try{
        val recoveryLists = parse(jsonString).extract[List[AutoRecoveryDataFormat]]
        recoveryLists.foreach(recovery => {
          log.info(s"${recovery.process},${recovery.model},${recovery.eqpid},${recovery.line},${recovery.triggeredBy}")
        })
        SaveAutoRecoveryBulk(recoveryLists)
      } catch {
        case ex: Throwable => log.warn(s"AutoRecoveryBulkData exception: ${ex.getMessage}, ${ex.getStackTraceString}")
      }

    case _ =>
  }

  def getProcessModel(eqpid: String): (String, String) = {
    var proc_model: (String, String) = null

    val eqpinfo_table = ServiceConfig.database.getCollection("EQP_INFO")
    val search = new BasicDBObject().append("eqpId", eqpid)
    val projection = new BasicDBObject().append("process", true).append("eqpModel", true).append("_id", false)
    val searchRet = eqpinfo_table.find(search).projection(projection).asScala
    if (searchRet.size == 1) {
      proc_model = (searchRet.head.getString("process"), searchRet.head.getString("eqpModel"))
    }
    proc_model
  }

  def SaveAutoRecoveryBulk(lists: List[AutoRecoveryDataFormat]): Unit = {
    val docLists = scala.collection.mutable.ListBuffer.empty[(Document, Int)]
    val txn = System.currentTimeMillis()
    val create_date = ISODateTimeFormat.dateTime().print(txn)

    lists.foreach(recovery => {
      try{
        val prop = getSCProperty(recovery.process, recovery.model, recovery.code)
        if (prop._1 == null) {
          log.warn(s"Scenario property is null: ${recovery.process}, ${recovery.model}, ${recovery.code}")
        } else {
          if(prop._1.IsEnabled){
            val retry = s"0/${prop._1.RetryCnt}/${prop._1.RetryInterval}"
            val paramsToEquip = recovery.params ++ Map("ALTXT" -> recovery.code, "ALID" -> recovery.code, "@Trigger" -> recovery.triggeredBy, "__priority__" -> prop._1.Priority)
            val params_doc = paramsToEquip.foldLeft(new Document())((doc, e) => doc.append(e._1, e._2))
            val newDoc = new Document().append("process", recovery.process).append("model", recovery.model).append("line", recovery.line).append("eqpid", recovery.eqpid).append("txn_seq", txn)
              .append("status", "Wait").append("ears_code", recovery.code).append("retry", retry).append("trigger_by", recovery.triggeredBy).append("create_date", create_date).append("params", params_doc)
            
            docLists.+=((newDoc, prop._1.StartDelay))

            context.actorSelection("/user/Master/MessageProducer") ! SendMessageToStatusTopic(recovery.process, recovery.model, recovery.eqpid, recovery.code, txn, "Wait", recovery.params)
          } else {
            log.info(s"Scenario is disabled: ${recovery.process}, ${recovery.model}, ${recovery.code}")
          }
        }
      } catch {
        case ex: Throwable => log.warn(s"Add recovery failed: ${ex.getMessage}")
      }
    })

    if (docLists.nonEmpty) {
      val recovery_table = ServiceConfig.database.getCollection("EQP_AUTO_RECOVERY")
      recovery_table.insertMany(docLists.map(_._1).asJava)

      docLists.foreach(doc => {
        val eqpid = doc._1.getString("eqpid")
        val process = doc._1.getString("process")
        val model = doc._1.getString("model")
        val code = doc._1.getString("ears_code")
        val line = doc._1.getString("line")

        val jsonParams = doc._1.get("params").asInstanceOf[Document].toJson
        val json_contents = JsonDataFormat.toJson(ScenarioCommand(eqpid, code, txn, code, jsonParams))

        if (doc._2 == 0) {
          SendRecovery(eqpid, process, model, code, txn, line, json_contents)
        }
        else {
          context.actorSelection("/user/Master/RedisActor") ! RedisSetExData(s"deferred@$process@$model@$eqpid@$line@$txn@${txn + doc._2 == 0.toLong * 1000}@$code", "1", doc._2.toLong)
          log.info(s"Starting Scenario is deferred: $eqpid, $txn, $code, ${doc._2} seconds")
        }
      })
    }
  }

  def SaveAutoRecovery(process: String, model: String, eqpid: String, line: String, code: String, txn: Long, params: Map[String, String], triggeredBy: String, altxt: String): Unit = {
    val prop = getSCProperty(process, model, code)
    if (prop._1 == null) {
      throw JobActorException(s"Scenario property is null: $process, $model, $code")
    }
    if(prop._1.IsEnabled) {
      val retry = s"0/${prop._1.RetryCnt}/${prop._1.RetryInterval}"
      val create_date = ISODateTimeFormat.dateTime().print(txn)
      val paramsToEquip = params ++ Map("ALTXT" -> altxt, "ALID" -> code, "@Trigger" -> triggeredBy, "__priority__" -> prop._1.Priority)
      val recovery_table = ServiceConfig.database.getCollection("EQP_AUTO_RECOVERY")

      val params_doc = paramsToEquip.foldLeft(new Document())((doc, e) => doc.append(e._1, e._2))
      val newDoc = new Document().append("process", process).append("model", model).append("line", line).append("eqpid", eqpid).append("txn_seq", txn)
        .append("status", "Wait").append("ears_code", code).append("retry", retry).append("trigger_by", triggeredBy).append("create_date", create_date).append("params", params_doc)
      recovery_table.insertOne(newDoc)

      context.actorSelection("/user/Master/MessageProducer") ! SendMessageToStatusTopic(process, model, eqpid, code, txn, "Wait", paramsToEquip)

      val jsonParams = json(DefaultFormats).write(paramsToEquip)
      val json_contents = JsonDataFormat.toJson(ScenarioCommand(eqpid, code, txn, code, jsonParams))

      val curtime = System.currentTimeMillis()
      if (Math.abs(curtime - txn) > ServiceConfig.RecoveryAllowableTimeLimit.toMillis) {
        log.warn(s"The difference between server and equip is so big: system-$curtime, eqptime-$txn")
        throw JobActorException("RecoveryAllowableTimeLimit")
      }

      if (prop._1.StartDelay == 0) {
        SendRecovery(eqpid, process, model, code, txn, line, json_contents)
      }
      else {
        context.actorSelection("/user/Master/RedisActor") ! RedisSetExData(s"deferred@$process@$model@$eqpid@$line@$txn@${txn + prop_1.StartDelay.toLong * 1000}@$code", "1", prop_1.StartDelay.toLong)
        log.info(s"Starting Scenario is deferred: $eqpid, $txn, $code, ${prop._1.StartDelay} seconds")
      }
    } else {
      log.info(s"Scenario is disabled: $process, $model, $code")
    }
  }

  def SendRecovery(eqpid: String, process: String, model: String, scname: String, txn: Long, line_id: String, json: String): Unit = {
    val register_var = ("VariableMap" -> "") ~ ("Id" -> "") ~ ("Step" -> -1) ~ ("Retry" -> 0)
    context.actorSelection("/user/Master/RedisActor") ! RedisSetData(s"VB-$eqpid", compact(render(register_var)))

    if (process == "PHOTO" && model == "NIKON_PH1_EXP") {
      context.actorSelection("/user/Master/RedisActor") ! RedisPubMessage(s"ScenarioCommand-${eqpid}", json)
      updateRecoveryStatus(eqpid, process, model, scname, txn, line_id, "StartPending")
      log.info(s"Start AutoRecovery by redis - process: $process, line: $line_id, model: $model, eqpid: $eqpid, scname: $scname, txn: $txn")
    } else {

      val response = RunAction(Function.RunScenario, process, model, eqpid, json)
      if(response == AgentStatus.CommandSuccess) {
        updateRecoveryStatus(eqpid, process, model, scname, txn, line_id, "StartPending")
        log.info(s"Start AutoRecovery - process: $process, line: $line_id, model: $model, eqpid: $eqpid, scname: $scname, txn: $txn")
      } else {
        updateRecoveryStatus(eqpid, process, model, scname, txn, line_id, "NotStarted")
        log.info(s"Start NG-AutoRecovery - process: $process, line: $line_id, model: $model, eqpid: $eqpid, scname: $scname, txn: $txn")
      }
    }
  }
}