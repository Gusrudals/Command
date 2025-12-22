package com.sec.eeg.ars.actor

import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import akka.pattern.ask
import com.mongodb.BasicDBObject
import com.sec.eeg.ars.data.ServiceConfig.database
import com.sec.eeg.ars.data._
import com.tibco.tibrv._
import com.typesafe.config.Config
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.collection.JavaConverters._

case class RVResponseTimeout(msg: String)

case class RVInfo(line_id:Int, line:String, service:Int, new_work:String, daemon:String, subject:String, dcoll_subject:String, secondary:String)

class RVWorker(conf: Config) extends Actor {
  implicit val formats = DefaultFormats
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  import scala.concurrent.ExecutionContext.Implicit.global
  implicit val timeout = Timeout(5 seconds)

  private val log = LoggerFactory.getLogger(classOf[RVWorker])
  private val machineIP = ServiceConfig.MyServiceAddress.substring(0, ServiceConfig.MyServiceAddress.indexOf(":"))
  private val senders = scala.collection.mutable.Map.empty[String,(ActorRef,akka.actor.Cencellable,TibrvDispatcher)]

  override def preStart() : Unit = {

    TibrvMsg.setStringEncoding("KSC5601")
    log.info(s"RVWorker preStart")
  }

  override def postStop() : Unit = {
    log.info(s"RVWorker postStop")
  }

  private def getStatus(msg: String) : String = {
    val status = ".*\\sSTATUS=([a-zA-Z]+).*".r
    val _msg = msg.replace("\r","").replace("\n","")
    _msg match {
      case status(result) =>
        return result
      case "" =>
        return "timeout"
      case _ =>
        return msg
    }
    return msg
  }

  private def getBCID(msg: String) : (Boolean,String) = {
    val bcid = ".*\\sBCID=([a-zA-Z0-9]+).*".r
    val _msg = msg.replace("\r","").replace("\n","")
    _msg match {
      case bcid(result) =>
        return (true,result)
      case "" =>
        return (false,"timeout")
      case _ =>
        return (false,msg)
    }
    return (false,msg)
  }

  private def sendRVMessage(rv: RVInfo, msg: TibrvMsg, line: String, rcvSubject: String, cons: scala.collection.mutable.Map[String,TibrvMsg]) : Unit = {
    try{
      val response_queue = new TibrvQueue()
      val transport = new TibrvRvdTransport(rv.service.toString, rv.net_work, rv.daemon)
      val dcoll_subject = rv.dcoll_subject
      val callBack = new TibrvMsgCallback {
        override def onMsg(tibrvListener: TibrvListener, tibrvMsg: TibrvMsg): Unit = {
          log.info(s"TibrvMsgCallback: ${rcvSubject.toString}")
          response_queue.synchronized{
            cons(rcvSubject) = tibrvMsg
            response_queue.notify()
          }
        }
      }
      val rvListener = new TibrvListener(response_queue, callBack, transport, rcvSubject, null)
      val dispatcher = new TibrvDispatcher(s"${rcvSubject}-dispatcher", response_queue)

      transport.send(msg)
      log.info(s"send ${rcvSubject}")
      response_queue.synchronized{
        response_queue.wait(3000)
      }
      log.info(s"wait done ${rcvSubject}")

      rvListener.destroy()
      dispatcher.destroy()
      transport.destroy()

    }
    catch{
      case ex: Throwable => log.error(s"sendRVMessage error: ${ex.getMessage}")
    }
  }

  def checkRVResponse(cons: scala.collection.mutable.Map[String,TibrvMsg]) : (Boolean,String) = {
    var details = ""
    var results = List.empty[Int]
    cons.foreach(subj => {
      if(subj._2 != null) {
        val rcvData = getStatus(subj._2.getField("DATA").data.toString)
        if(rcvData == "PASS") {
          results = 1 :: results
        }else{
          details = details + rcvData + ","
        }
      }
    })
    if(results.length == cons.size){
      return (true,"")
    }
    else{
      return (false,details)
    }
  }

  def getRVInfo(line: String) : RVInfo = {
    var rvInfo : RVInfo = null
    var rv_table = database.getCollection("RV_INFO")
    val search = new BasicDBObject().append("line",line)
    val searchCursor = rv_table.find(search)
    val rvCols = searchCursor.asScala
    if(rvCols.size == 1){
      rvInfo = RVInfo(rvCols.head.getLong("lineId").toInt, rvCols.head.getString("line"), rvCols.head.getLong("service").toInt, rvCols.head.getString("netWork"),
        rvCols.head.getString("daemon"), rvCols.head.getString("subject"), rvCols.head.getString("dcollSubject"), rvCols.head.getString("secondary"))
    }
    rvInfo
  }

  def checkValidEqpid(eqpid: String) : Boolean = {
    var result = false
    val regexEqpid = "[A-Z0-9]+(?:-[1-9A-Z])?".r
    eqpid match {
      case regexEqpid() =>
        result = true
      case _ =>
        result = false
    }
    return result
  }


  def FutureRequestDownMsg(eqpid:String, line: String, txn: Long, dncode: String) : Future[(Boolean,String)] = Future{
    var retFuture = (false,"Unknown")
    log.info(s"RequestDownMsg:${eqpid},${line},${txn},${dncode}")
    var json = (("result" -> "success") ~ ("message" -> ""))
    try{
      val rvInfo = getRVInfo(line)
      if(rvInfo == null){
        log.warn("FutureRequestDownMsg: Data does not exists in DB")
        retFuture = (false,"Data does not exists in DB")
      }
      else {
        if (checkValidEqpid(eqpid)) {
          val tibrvCons = scala.collection.mutable.Map.empty[String, TibrvMsg]
          val rcvSubject = s"${line}.LH.EARS.${eqpid}${txn}"
          tibrvCons += ((rcvSubject, null))

          val rvMsg = s"""EQPSTAT HDR=(${rvInfo.dcoll_subject},${rcvSubject},EQDOWN) EQPID=${eqpid} OPERID=EARSAUTO MODE=DOWN EQPSTATUS=DOWN CODE=${dncode} COMMENT="This is a auto equipment down message.""""
          log.info(s"RequestDownMsg MSG: ${rvMsg}")
          val sendmsg = new TibrvMsg()
          sendmsg.setSendSubject(rvInfo.dcoll_subject)
          sendmsg.setReplySubject(rcvSubject)
          sendmsg.update("DATA", rvMsg)
          sendRVMessage(rvInfo sendmsg, line, rcvSubject, tibrvCons)
          retFuture = checkRVResponse(tibrvCons)
        } else {
          retFuture = (false,"EQPID is Wrong")
        }
      }
    }
    catch {
      case ex: Throwable =>
        log.warn(s"FutureRequestDownMsg: ${ex.getMessage}")
        retFuture = (false,ex.getMessage)

    }

    retFuture
  }

  def FutureRequestReleaseMsg(eqpid:String, line: String, txn: Long) : Future[(Boolean,String)] = Future{
    var retFuture = (false,"Unknown")
    log.info(s"RequestReleaseMsg:${eqpid},${line},${txn}")
    var json = (("result" -> "success") ~ ("message" -> ""))
    try{
      val rvInfo = getRVInfo(line)
      if(rvInfo == null){
        log.warn("FutureRequestReleaseMsg: Data does not exists in DB")
        retFuture = (false,"Data does not exists in DB")
      }

      else {
        if (checkValidEqpid(eqpid)) {
          val tibrvCons = scala.collection.mutable.Map.empty[String, TibrvMsg]
          val rcvSubject = s"${line}.LH.EARS.${eqpid}${txn}"
          tibrvCons += ((rcvSubject, null))
          val rvMsg = s"""EQPSTAT HDR=(${rvInfo.dcoll_subject},${rcvSubject},EQPSTAT) EQPID=${eqpid} OPERID=EARSAUTO MODE=PROD EQPSTATUS=IDLE CODE=A01 RELEASEDOWNTXN=${txn} COMMENT="This is a auto equipment auto recovery release message.""""
          log.info(s"RequestReleaseMsg MSG: ${rvMsg}")
          val sendmsg = new TibrvMsg()
          sendmsg.setSendSubject(rvInfo.dcoll_subject)
          sendmsg.setReplySubject(rcvSubject)
          sendmsg.update("DATA", rvMsg)
          sendRVMessage(rvInfo sendmsg, line, rcvSubject, tibrvCons)
          retFuture = checkRVResponse(tibrvCons)
        } else {
          retFuture = (false,"EQPID is Wrong")
        }
      }
    }
    catch {
      case ex: Throwable =>
        log.warn(s"FutureRequestReleaseMsg: ${ex.getMessage}")
        retFuture = (false,ex.getMessage)

    }

    retFuture
  }

  def FutureGetServerRestartTC(eqpid:String, line: String, txn: Long) : Future[(Boolean,String)] = Future{
    var retFuture = (false,"Unknown")
    log.info(s"GetServerRestartTC:${eqpid},${line},${txn}")
    var json = (("result" -> "success") ~ ("message" -> ""))
    try{
      val rvInfo = getRVInfo(line)
      if(rvInfo == null){
        log.warn("GetServerRestartTC: Data does not exists in DB")
        retFuture = (false,"Data does not exists in DB")
      }
      else {
        if (checkValidEqpid(eqpid)) {
          val tibrvCons = scala.collection.mutable.Map.empty[String, TibrvMsg]
          val rcvSubject = s"${line}.LH.EARS.${eqpid}${txn}"
          val Subject = s"${line}.LH.SIDEmgr"
          tibrvCons += ((rcvSubject, null))
          val rvMsg = s"""REMOTE_COMMAND_REQUEST HDR=(${Subject},${rcvSubject},EARS_REQUEST) EQPID=${eqpid} OPERID=EARSAUTO TRACKINGTYPE=REQUEST"""
          log.info(s"GetServerRestartTC MSG: ${rvMsg}")
          val sendmsg = new TibrvMsg()
          sendmsg.setSendSubject(Subject)
          sendmsg.setReplySubject(rcvSubject)
          sendmsg.update("DATA", rvMsg)
          sendRVMessage(rvInfo sendmsg, line, rcvSubject, tibrvCons)
          val _retFuture = checkRVResponse(tibrvCons)
          retFuture = getBCID(tibrvCons(rcvSubject).getField("DATA").data.toString)
        } else {
          retFuture = (false,"EQPID is Wrong")
        }
      }
    }
    catch {
      case ex: Throwable =>
        log.warn(s"GetServerRestartTC: ${ex.getMessage}")
        retFuture = (false,ex.getMessage)
    }

    retFuture
  }

  def FutureRequestServerRestart(eqpid:String, line: String, txn: Long, bcid: String) : Future[(Boolean,String)] = Future{
    var retFuture = (false,"Unknown")
    log.info(s"RequestServerRestart:${eqpid},${line},${txn}")
    var json = (("result" -> "success") ~ ("message" -> ""))
    try{
      val rvInfo = getRVInfo(line)
      if(rvInfo == null){
        log.warn("RequestServerRestart: Data does not exists in DB")
        retFuture = (false,"Data does not exists in DB")
      }
      else {
        if (checkValidEqpid(eqpid)) {
          val tibrvCons = scala.collection.mutable.Map.empty[String, TibrvMsg]
          val rcvSubject = s"${line}.LH.EARS.${eqpid}${txn}"
          val Subject = s"${line}.LH.${bcid}"
          tibrvCons += ((rcvSubject, null))
          val rvMsg = s"""SRVRESTART HDR=(${bcid},${rcvSubject},EARS_RESTART) EQPID=${eqpid} OPERID=EARSAUTO"""
          log.info(s"RequestServerRestart MSG: ${rvMsg}")
          val sendmsg = new TibrvMsg()
          sendmsg.setSendSubject(Subject)
          sendmsg.setReplySubject(rcvSubject)
          sendmsg.update("DATA", rvMsg)
          sendRVMessage(rvInfo sendmsg, line, rcvSubject, tibrvCons)
          retFuture = checkRVResponse(tibrvCons)
        } else {
          retFuture = (false,"EQPID is Wrong")
        }
      }
    }
    catch {
      case ex: Throwable =>
        log.warn(s"RequestServerRestart: ${ex.getMessage}")
        retFuture = (false,ex.getMessage)
    }

    retFuture
  }

  def FuturePMStartMsg(pmtype:String, eqpid:String, line: String, chamberid: String) : Future[(Boolean,String)] = Future{
    var retFuture = (false,"Unknown")
    val _chamberID = if(chamberid=="") "-" else chamberid

    log.info(s"[FuturePMStartMsg] start ${pmtype},${eqpid},${line},${_chamberID}")
    var json = (("result" -> "success") ~ ("message" -> ""))
    try{
      val rvInfo = getRVInfo(line)
      if(rvInfo == null){
        log.warn(s"[FuturePMStartMsg] ${pmtype},${eqpid},${line},${_chamberID}: Data does not exists in DB")
        retFuture = (false,"Data does not exists in DB")
      }
      else {
        if (checkValidEqpid(eqpid)) {
          val curtimestamp = System.currentTimeMillis()
          val tibrvCons = scala.collection.mutable.Map.empty[String, TibrvMsg]
          val rcvSubject = s"${line}.LH.EARS.${eqpid}${curtimestamp}"
          tibrvCons += ((rcvSubject, null))
          val date = new DateTime()
          val rvMsg = s"""MACROSTARTED HDR=(TRACKING,${rcvSubject},${pmtype},${date.toString("yyyyMMddHHmmssSSS")}) EQPID=${eqpid} OPERID=EARSAUTO STATUS=PASS CHAMBERID=${_chamberID} MACRO_ID=${pmtype}"""
          log.info(s"[FuturePMStartMsg] MSG ${pmtype},${eqpid},${line},${_chamberID}: ${rvMsg}")
          val sendmsg = new TibrvMsg()
          sendmsg.setSendSubject(rvInfo.dcoll_subject)
          sendmsg.setReplySubject(rcvSubject)
          sendmsg.update("DATA", rvMsg)
          sendRVMessage(rvInfo sendmsg, line, rcvSubject, tibrvCons)
          retFuture = checkRVResponse(tibrvCons)
        } else {
          retFuture = (false,"EQPID is Wrong")
        }
      }
    }
    catch {
      case ex: Throwable =>
        log.warn(s"[FuturePMStartMsg] exception ${pmtype},${eqpid},${line},${_chamberID}: ${ex.getMessage}")
        retFuture = (false,ex.getMessage)
    }

    retFuture
  }

  def FutureSSBActionMsg(eqpid:String, line: String, smlid: String) : Future[(Boolean,String)] = Future{
    var retFuture = (false,"Unknown")
    log.info(s"[FutureSSBActionMsg] start ${eqpid},${line},${smlid}")
    var json = (("result" -> "success") ~ ("message" -> ""))
    try{
      val rvInfo = getRVInfo(line)
      if(rvInfo == null){
        log.warn(s"[FutureSSBActionMsg] ${eqpid},${line},${smlid}: Data does not exists in DB")
        retFuture = (false,"Data does not exists in DB")
      }
      else {
        if (checkValidEqpid(eqpid)) {
          val curtimestamp = System.currentTimeMillis()
          val tibrvCons = scala.collection.mutable.Map.empty[String, TibrvMsg]
          val rcvSubject = s"${line}.LH.EARS.${eqpid}${curtimestamp}"
          tibrvCons += ((rcvSubject, null))
          val rvMsg = s"""SSBACTION HDR=(TRACKING,${rcvSubject},SSBACTION) EQPID=${eqpid} OPERID=EARSAUTO SMLID=${smlid} INFORMID= TITLE= TYPE=CHANGEEVENT"""
          log.info(s"[FutureSSBActionMsg] MSG ${eqpid},${line},${smlid}: ${rvMsg}")
          val sendmsg = new TibrvMsg()
          sendmsg.setSendSubject(rvInfo.dcoll_subject)
          sendmsg.setReplySubject(rcvSubject)
          sendmsg.update("DATA", rvMsg)
          sendRVMessage(rvInfo sendmsg, line, rcvSubject, tibrvCons)
          retFuture = checkRVResponse(tibrvCons)
        } else {
          retFuture = (false,"EQPID is Wrong")
        }
      }
    }
    catch {
      case ex: Throwable =>
        log.warn(s"[FutureSSBActionMsg] exception ${eqpid},${line},${smlid}: ${ex.getMessage}")
        retFuture = (false,ex.getMessage)
    }

    retFuture
  }

  def FutureACSCallMsg(eqpid:String, line: String, comments: String) : Future[(Boolean,String)] = Future{
    var retFuture = (false,"Unknown")

    log.info(s"[FutureACSCallMsg] start ${eqpid},${line},${comments}")
    var json = (("result" -> "success") ~ ("message" -> ""))
    try{
      val rvInfo = getRVInfo(line)
      if(rvInfo == null){
        log.warn(s"[FutureACSCallMsg] ${eqpid},${line},${comments}: Data does not exists in DB")
        retFuture = (false,"Data does not exists in DB")
      }
      else {
        if (checkValidEqpid(eqpid)) {
          val curtimestamp = System.currentTimeMillis()
          val tibrvCons = scala.collection.mutable.Map.empty[String, TibrvMsg]
          val rcvSubject = s"${line}.LH.EARS.${eqpid}${curtimestamp}"
          tibrvCons += ((rcvSubject, null))
          val date = new DateTime()
          val rvMsg = s"""OILOG HDR=(TRACKING,${rcvSubject},${eqpid},${date.toString("yyyyMMddHHmmssSSS")}) OSSNAME=EARS TYPE=EARES EQPID=${eqpid} LOTID=_ OPERID=EARSAUTO OPERNAME=EARS_AUTO COMMENT="${comments}""""
          log.info(s"[FutureACSCallMsg] MSG ${eqpid},${line},${comments}: ${rvMsg}")
          val sendmsg = new TibrvMsg()
          sendmsg.setSendSubject(rvInfo.dcoll_subject)
          sendmsg.setReplySubject(rcvSubject)
          sendmsg.update("DATA", rvMsg)
          sendRVMessage(rvInfo sendmsg, line, rcvSubject, tibrvCons)
          retFuture = checkRVResponse(tibrvCons)
        } else {
          retFuture = (false,"EQPID is Wrong")
        }
      }
    }
    catch {
      case ex: Throwable =>
        log.warn(s"[FutureACSCallMsg] exception ${eqpid},${line},${comments}: ${ex.getMessage}")
        retFuture = (false,ex.getMessage)
    }

    retFuture
  }


  def FuturePMCompleteMsg(pmtype:String, eqpid:String, line: String, chamberid: String) : Future[(Boolean,String)] = Future{
    var retFuture = (false,"Unknown")
    val _chamberID = if(chamberid=="") "-" else chamberid

    log.info(s"[FuturePMCompleteMsg] start ${pmtype},${eqpid},${line},${_chamberID}")
    var json = (("result" -> "success") ~ ("message" -> ""))
    try{
      val rvInfo = getRVInfo(line)
      if(rvInfo == null){
        log.warn(s"[FuturePMCompleteMsg] ${pmtype},${eqpid},${line},${_chamberID}: Data does not exists in DB")
        retFuture = (false,"Data does not exists in DB")
      }
      else {
        if (checkValidEqpid(eqpid)) {
          val curtimestamp = System.currentTimeMillis()
          val tibrvCons = scala.collection.mutable.Map.empty[String, TibrvMsg]
          val rcvSubject = s"${line}.LH.EARS.${eqpid}${curtimestamp}"
          tibrvCons += ((rcvSubject, null))
          val date = new DateTime()
          val rvMsg = s"""MACROCOMPLETED HDR=(TRACKING,${rcvSubject},${pmtype},${date.toString("yyyyMMddHHmmssSSS")}) EQPID=${eqpid} OPERID=EARSAUTO CHAMBERID=${_chamberID} MODE=END MACRO_ID=${pmtype} CHECKRESULT"""
          log.info(s"[FuturePMCompleteMsg] MSG ${pmtype},${eqpid},${line},${_chamberID}: ${rvMsg}")
          val sendmsg = new TibrvMsg()
          sendmsg.setSendSubject(rvInfo.dcoll_subject)
          sendmsg.setReplySubject(rcvSubject)
          sendmsg.update("DATA", rvMsg)
          sendRVMessage(rvInfo sendmsg, line, rcvSubject, tibrvCons)
          retFuture = checkRVResponse(tibrvCons)
        } else {
          retFuture = (false,"EQPID is Wrong")
        }
      }
    }
    catch {
      case ex: Throwable =>
        log.warn(s"[FuturePMCompleteMsg] exception ${pmtype},${eqpid},${line},${_chamberID}: ${ex.getMessage}")
        retFuture = (false,ex.getMessage)
    }

    retFuture
  }

  def FutureLoadPortAutoJobMsg(eqpid:String, line: String, comments: String, portLists: List[String], autoFlag: String) : Future[(Boolean,String)] = Future{
    var retFuture = (false,"Unknown")
    var portsString = ""
    portLists.foreach(port => {
      if(port != ""){
        if(portsString != ""){
          portsString += ","
        }
        portsString += port
      }
    })

    log.info(s"[FutureLoadPortAutoJobMsg] start ${eqpid},${portsString},${line},${comments},${autoFlag}")

    if(portsString == "" || (autoFlag != "Y" && autoFlag != "N")  ){
      log.warn(s"[FutureLoadPortAutoJobMsg] ${eqpid},${portsString},${line},${comments},${autoFlag}: Invalid parameters" )
      retFuture = (false, "Invalid parameters")
    }else{
      var json = (("result" -> "success") ~ ("message" -> ""))
      try{
        val rvInfo = getRVInfo(line)
        if(rvInfo == null){
          log.warn(s"[FutureLoadPortAutoJobMsg] ${eqpid},${portsString},${line},${comments},${autoFlag}: Data does not exists in DB")
          retFuture = (false,"Data does not exists in DB")
        }
        else {
          if (checkValidEqpid(eqpid)) {
            val curtimestamp = System.currentTimeMillis()
            val tibrvCons = scala.collection.mutable.Map.empty[String, TibrvMsg]
            val rcvSubject = s"${line}.LH.EARS.${eqpid}${curtimestamp}"
            tibrvCons += ((rcvSubject, null))
            val date = new DateTime()
            val rvMsg = s"""EQP_PARAMETER_REQUEST HDR=(NCmgr,${rcvSubject},AUTOFLAG) EQPID= OPERID=EARSAUTO TYPE=FULLAUTO PORTID=${portsString} AUTOJOBFLAG=${autoFlag} COMMENT=":${comments}""""
            log.info(s"[FutureLoadPortAutoJobMsg] MSG ${eqpid},${portsString},${line},${comments},${autoFlag}: ${rvMsg}")
            val sendmsg = new TibrvMsg()
            sendmsg.setSendSubject(s"${line}.LH.NCmgr")
            sendmsg.setReplySubject(rcvSubject)
            sendmsg.update("DATA", rvMsg)
            sendRVMessage(rvInfo sendmsg, line, rcvSubject, tibrvCons)
            retFuture = checkRVResponse(tibrvCons)
          } else {
            retFuture = (false,"EQPID is Wrong")
          }
        }
      }
      catch {
        case ex: Throwable =>
          log.warn(s"[FutureLoadPortAutoJobMsg] exception ${eqpid},${portsString},${line},${comments},${autoFlag}: ${ex.getMessage}")
          retFuture = (false,ex.getMessage)
      }
    }
    retFuture
  }


  def FutureRVNormalMsg(eqpid:String, line: String, smlid: String) : Future[(Boolean,String)] = Future{
    var retFuture = (false,"Unknown")
    log.info(s"[FutureSSBActionMsg] start ${eqpid},${line},${smlid}")
    var json = (("result" -> "success") ~ ("message" -> ""))
    try{
      val rvInfo = getRVInfo(line)
      if(rvInfo == null){
        log.warn(s"[FutureSSBActionMsg] ${eqpid},${line},${smlid}: Data does not exists in DB")
        retFuture = (false,"Data does not exists in DB")
      }
      else {
        if (checkValidEqpid(eqpid)) {
          val curtimestamp = System.currentTimeMillis()
          val tibrvCons = scala.collection.mutable.Map.empty[String, TibrvMsg]
          val rcvSubject = s"${line}.LH.EARS.${eqpid}${curtimestamp}"
          tibrvCons += ((rcvSubject, null))
          val rvMsg = s"""SSBACTION HDR=(TRACKING,${rcvSubject},SSBACTION) EQPID=${eqpid} OPERID=EARSAUTO SMLID=${smlid} INFORMID= TITLE= TYPE=CHANGEEVENT"""
          log.info(s"[FutureSSBActionMsg] MSG ${eqpid},${line},${smlid}: ${rvMsg}")
          val sendmsg = new TibrvMsg()
          sendmsg.setSendSubject(rvInfo.dcoll_subject)
          sendmsg.setReplySubject(rcvSubject)
          sendmsg.update("DATA", rvMsg)
          sendRVMessage(rvInfo sendmsg, line, rcvSubject, tibrvCons)
          retFuture = checkRVResponse(tibrvCons)
        } else {
          retFuture = (false,"EQPID is Wrong")
        }
      }
    }
    catch {
      case ex: Throwable =>
        log.warn(s"[FutureSSBActionMsg] exception ${eqpid},${line},${smlid}: ${ex.getMessage}")
        retFuture = (false,ex.getMessage)
    }

    retFuture
  }

  override def receive : Receive = {


      case RequesetTkinCancel(eqpid, line, txn) =>
        val _sender = sender()

      case RequestEquipmentDown(eqpid, line, txn, dncode) =>
        val _sender = sender()

        try{
          FutureRequestDownMsg(eqpid,line,txn,dncode).onComplete(ret => {
            val _ret = ret.getOrElse((false,"Unknown"))
            if(_ret._1){
              val json = (("result" -> "Success") ~ ("message" -> ""))
              _sender ! compact(render(json))
            }
            else{
              val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
              _sender ! compact(render(json))
            }
          })
        }
        catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }
      

      case RequestReleaseMsg(eqpid, line, txn) =>
        val _sender = sender()

        try{
          FutureRequestReleaseMsg(eqpid,line,txn).onComplete(ret => {
            val _ret = ret.getOrElse((false,"Unknown"))
            if(_ret._1){
              val json = (("result" -> "Success") ~ ("message" -> ""))
              _sender ! compact(render(json))
            }
            else{
              val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
              _sender ! compact(render(json))
            }
          })
        }
        catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }

      case RequestServerRestart(eqpid, line, txn) =>
        val _sender = sender()

        try{
          FutureGetServerRestartTC(eqpid,line,txn).onComplete(ret => {
            val _ret = ret.getOrElse((false,"Unknown"))
            if(_ret._1){
              FutureRequestServerRestart(eqpid,line,txn,_ret._2)onComplete(ret2 => {
                val _ret2 = ret.getOrElse((false,"Unknown"))
                if(_ret2._1){
                  val json = (("result" -> "Success") ~ ("message" -> ""))
                  _sender ! compact(render(json))
                }
                else{
                  val json = (("result" -> "Failed") ~ ("message" -> _ret2._2))
                  _sender ! compact(render(json))
                }
              })
            }
            else{
              val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
              _sender ! compact(render(json))
            }
          })
        }
        catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }

      case PrePMStart(eqpid,line,chamberid,pmcode) =>
        val _sender = sender()
        try{
          val _key = s"PMHistory:${line}-${eqpid}:${chamberid}:${pmcode}"
          val future = context.actorSelection("/user/Master/RedisActor") ? RedisGetDataPM(_key)
          val results = Await.result(future, timeout.duration)
          if(results != null && results.asInstanceOf[String] == "PrePMStart"){
            val failmsg = s"This RV Request is already executed. so this RV request is ignored: ${_key}, PrePMStart"
            log.info(failmsg)
            val json = (("result" -> "Failed") ~ ("message" -> failmsg))
            _sender ! compact(render(json))
          }else{
            FuturePMStartMsg("PREPM",eqpid,line,chamberid).onComplete(ret => {
              val _ret = ret.getOrElse((false,"Unknown"))
              if(_ret._1){
                context.actorSelection("/user/Master/RedisActor") ! RedisSetDataPM(_key,"PrePMStart",86400)
                val json = (("result" -> "Success") ~ ("message" -> ""))
                _sender ! compact(render(json))
              }
              else{
                val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
                _sender ! compact(render(json))
              }
            })
          }

        }catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }

      case PrePMComplete(eqpid,line,chamberid,pmcode) =>
        val _sender = sender()
        try{
          val _key = s"PMHistory:${line}-${eqpid}:${chamberid}:${pmcode}"
          val future = context.actorSelection("/user/Master/RedisActor") ? RedisGetDataPM(_key)
          val results = Await.result(future, timeout.duration)
          if(results != null && results.asInstanceOf[String] == "PrePMComplete"){
            val failmsg = s"This RV Request is already executed. so this RV request is ignored: ${_key}, PrePMComplete"
            log.info(failmsg)
            val json = (("result" -> "Failed") ~ ("message" -> failmsg))
            _sender ! compact(render(json))
          }else{
            FuturePMCompleteMsg("PREPM",eqpid,line,chamberid).onComplete(ret => {
              val _ret = ret.getOrElse((false,"Unknown"))
              if(_ret._1){
                context.actorSelection("/user/Master/RedisActor") ! RedisSetDataPM(_key,"PrePMComplete",86400)
                val json = (("result" -> "Success") ~ ("message" -> ""))
                _sender ! compact(render(json))
              }
              else{
                val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
                _sender ! compact(render(json))
              }
            })
          }

        }catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }

      case PhysicalPMStart(eqpid, line, chamberid,pmcode) =>
        val _sender = sender()
        try{
          val _key = s"PMHistory:${line}-${eqpid}:${chamberid}:${pmcode}"
          val future = context.actorSelection("/user/Master/RedisActor") ? RedisGetDataPM(_key)
          val results = Await.result(future, timeout.duration)
          if(results != null && results.asInstanceOf[String] == "PhysicalPMStart"){
            val failmsg = s"This RV Request is already executed. so this RV request is ignored: ${_key}, PhysicalPMStart"
            log.info(failmsg)
            val json = (("result" -> "Failed") ~ ("message" -> failmsg))
            _sender ! compact(render(json))
          }else{
            FuturePMStartMsg("PHYSICAL",eqpid,line,chamberid).onComplete(ret => {
              val _ret = ret.getOrElse((false,"Unknown"))
              if(_ret._1){
                context.actorSelection("/user/Master/RedisActor") ! RedisSetDataPM(_key,"PhysicalPMStart",86400)
                val json = (("result" -> "Success") ~ ("message" -> ""))
                _sender ! compact(render(json))
              }
              else{
                val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
                _sender ! compact(render(json))
              }
            })
          }

        }catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }

      case PhysicalPMComplete(eqpid, line, chamberid,pmcode) =>
        val _sender = sender()
        try{
          val _key = s"PMHistory:${line}-${eqpid}:${chamberid}:${pmcode}"
          val future = context.actorSelection("/user/Master/RedisActor") ? RedisGetDataPM(_key)
          val results = Await.result(future, timeout.duration)
          if(results != null && results.asInstanceOf[String] == "PhysicalPMComplete"){
            val failmsg = s"This RV Request is already executed. so this RV request is ignored: ${_key}, PhysicalPMComplete"
            log.info(failmsg)
            val json = (("result" -> "Failed") ~ ("message" -> failmsg))
            _sender ! compact(render(json))
          }else{
            FuturePMCompleteMsg("PHYSICAL",eqpid,line,chamberid).onComplete(ret => {
              val _ret = ret.getOrElse((false,"Unknown"))
              if(_ret._1){
                context.actorSelection("/user/Master/RedisActor") ! RedisSetDataPM(_key,"PhysicalPMComplete",86400)
                val json = (("result" -> "Success") ~ ("message" -> ""))
                _sender ! compact(render(json))
              }
              else{
                val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
                _sender ! compact(render(json))
              }
            })
          }

        }catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }
      
      case PostPMStart(eqpid,line,chamberid,pmcode) =>
        val _sender = sender()
        try{
          val _key = s"PMHistory:${line}-${eqpid}:${chamberid}:${pmcode}"
          val future = context.actorSelection("/user/Master/RedisActor") ? RedisGetDataPM(_key)
          val results = Await.result(future, timeout.duration)
          if(results != null && results.asInstanceOf[String] == "PostPMStart"){
            val failmsg = s"This RV Request is already executed. so this RV request is ignored: ${_key}, PostPMStart"
            log.info(failmsg)
            val json = (("result" -> "Failed") ~ ("message" -> failmsg))
            _sender ! compact(render(json))
          }else{
            FuturePMStartMsg("POSTPM",eqpid,line,chamberid).onComplete(ret => {
              val _ret = ret.getOrElse((false,"Unknown"))
              if(_ret._1){
                context.actorSelection("/user/Master/RedisActor") ! RedisSetDataPM(_key,"PostPMStart",86400)
                val json = (("result" -> "Success") ~ ("message" -> ""))
                _sender ! compact(render(json))
              }
              else{
                val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
                _sender ! compact(render(json))
              }
            })
          }

        }catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }

      case PostPMComplete(eqpid,line,chamberid,pmcode) =>
        val _sender = sender()
        try{
          val _key = s"PMHistory:${line}-${eqpid}:${chamberid}:${pmcode}"
          val future = context.actorSelection("/user/Master/RedisActor") ? RedisGetDataPM(_key)
          val results = Await.result(future, timeout.duration)
          if(results != null && results.asInstanceOf[String] == "PostPMComplete"){
            val failmsg = s"This RV Request is already executed. so this RV request is ignored: ${_key}, PostPMComplete"
            log.info(failmsg)
            val json = (("result" -> "Failed") ~ ("message" -> failmsg))
            _sender ! compact(render(json))
          }else{
            FuturePMCompleteMsg("POSTPM",eqpid,line,chamberid).onComplete(ret => {
              val _ret = ret.getOrElse((false,"Unknown"))
              if(_ret._1){
                context.actorSelection("/user/Master/RedisActor") ! RedisSetDataPM(_key,"PostPMComplete",86400)
                val json = (("result" -> "Success") ~ ("message" -> ""))
                _sender ! compact(render(json))
              }
              else{
                val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
                _sender ! compact(render(json))
              }
            })
          }

        }catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }

      case SSBAction(eqpid,line,smlid) =>
        val _sender = sender()

        try{
          FutureSSBActionMsg(eqpid,line,smlid).onComplete(ret => {
            val _ret = ret.getOrElse((false,"Unknown"))
            if(_ret._1){
              val json = (("result" -> "Success") ~ ("message" -> ""))
              _sender ! compact(render(json))
            }
            else{
              val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
              _sender ! compact(render(json))
            }
          })
        }
        catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }

      case ACSCall(eqpid,line,comments) =>
        val _sender = sender()

        try{
          FutureACSCallMsg(eqpid,line,comments).onComplete(ret => {
            val _ret = ret.getOrElse((false,"Unknown"))
            if(_ret._1){
              val json = (("result" -> "Success") ~ ("message" -> ""))
              _sender ! compact(render(json))
            }
            else{
              val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
              _sender ! compact(render(json))
            }
          })
        }
        catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }

      case LoadPortAutoJob(eqpid,line,comments,portLists,autoFlag) =>
        val _sender = sender()

        try{
          FutureLoadPortAutoJobMsg(eqpid,line,comments,portLists,autoFlag).onComplete(ret => {
            val _ret = ret.getOrElse((false,"Unknown"))
            if(_ret._1){
              val json = (("result" -> "Success") ~ ("message" -> ""))
              _sender ! compact(render(json))
            }
            else{
              val json = (("result" -> "Failed") ~ ("message" -> _ret._2))
              _sender ! compact(render(json))
            }
          })
        }
        catch{
          case ex : Exception =>
            val json = (("result" -> "Failed") ~ ("message" -> ex.getMessage))
            _sender ! compact(render(json))
        }
      
      case _ =>
        val json = (("result" -> "Failed") ~ ("message" -> "Invalid data format"))
        sender() ! compact(render(json))
  }
}