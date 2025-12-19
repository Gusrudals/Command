package com.sec.eeg.ars.actor

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.HttpOriginRange
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import ch.megard.akka.http.cors.scaladsl.model.HttpHeaderRange
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import com.sec.eeg.ars.actor.Master.ShutDown
import com.sec.eeg.ars.data._
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read
import org.slf4j.LoggerFactory
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object HttpWorker {
  private val log = LoggerFactory.getLogger(classOf[HttpWorker])

  implicit val system = Master.system
  implicit val askTimeout: Timeout = 5 seconds
  implicit val ActorMaterializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  implicit val itemFormat1 = jsonFormat5(NewDataFromLocal)
  implicit val itemFormat2 = jsonFormat6(NewDataFromLocalWithParams)
  implicit val itemFormat3 = jsonFormat7(AgentEvent)
  implicit val itemFormat4 = jsonFormat8(RequestEmailTrans)
  implicit val itemFormat5 = jsonFormat12(ScenarioResult)
  implicit val itemFormat6 = jsonFormat13(ScenarioDetailsResult)
  implicit val itemFormat7 = jsonFormat14(ScenarioDetailsResultWithMap)
  implicit val itemFormat8 = jsonFormat3(RequestTkinCancel)
  implicit val itemFormat9 = jsonFormat3(RequestReleaseMsg)
  implicit val itemFormat10 = jsonFormat3(RequestServerRestart)
  implicit val itemFormat11 = jsonFormat4(RequestEquipmentDown)
  implicit val itemFormat12 = jsonFormat4(PrePMStart)
  implicit val itemFormat13 = jsonFormat4(PrePMComplete)
  implicit val itemFormat14 = jsonFormat4(PhysicalPMStart)
  implicit val itemFormat15 = jsonFormat4(PhysicalPMComplete)
  implicit val itemFormat16 = jsonFormat4(PostPMStart)
  implicit val itemFormat17 = jsonFormat4(PostPMComplete)
  implicit val itemFormat18 = jsonFormat4(SSBAction)
  implicit val itemFormat19 = jsonFormat3(ACSCall)
  implicit val itemFormat20 = jsonFormat5(LoadPortAutoJob)
  implicit val formats = DefaultFormats

  val settings = CorsSettings.defaultSettings.withAllowGenericHttpRequests(true).withAllowedOrigins(HttpOriginRange.*).withAllowedHeaders(HttpHeaderRange.*)

  val route = cors(settings) {
    path("EARS" / "Agent") {
      post {
        (extractRequest & extractClientIP & entity(as[String])) { (request, ip, body) =>
          log.info(s"/EARS/Agent request received: ${ip.toOption.map(_.getHostAddress).getOrElse("unknown")}")
          val headerMap = request.headers.map(h => (h.name(),h.value())).toMap
          headerMap.get("function") match {
            case Some("newdatafromlocal") =>
              val conv = read[NewDataFromLocal](body)
              log.info(s"newdatafromlocal: ${conv.hostname}, ${conv.txn}")
              system.actorSelection("/user/Master/JobActor") ! conv
              complete("ok")
            case Some("newdatafromlocalwithparams") =>
              val conv = read[NewDataFromLocalWithParams](body)
              log.info(s"newdatafromlocalwithparams: ${conv.hostname}, ${conv.txn}")
              system.actorSelection("/user/Master/JobActor") ! conv
              complete("ok")
            case Some("updatestatus") =>
              val conv = read[AgentEvent](body)
              log.info(s"updatestatus: ${conv.hostname}, ${conv.txn}")
              system.actorSelection("/user/Master/JobActor") ! conv
              complete("ok")
            case Some("sendemail") =>
              val conv = read[RequestEmailTrans](body)
              val exists = if(conv.snapshot != null && conv.snapshot != "") true else false
              log.info(s"sendemail: ${conv.hostname}, ${conv.ip}, ${conv.process}, ${conv.process}, ${conv.line}, ${conv.model}, ${conv.code}, ${conv.subcode}, ${exists}")
              system.actorSelection("/user/Master/JobActor") ! conv
              complete("ok")
            case Some("scenarioresult") =>
              val conv = read[ScenarioResult](body)
              log.info(s"scenarioresult: ${conv.hostname}, ${conv.txn}")
              val conv2 = ScenarioDetailsResultWithMap(conv.hostname, conv.process, conv.model, conv.line, conv.diff, conv.scname, conv.result, conv.altxt, conv.currentstep, conv.snapshot, conv.txn, conv.ts, "", Map.empty[String,String])
              system.actorSelection("/user/Master/JobActor") ! conv2
              complete("ok")
            case Some("scenariodetailsresult") =>
              val conv = read[ScenarioDetailsResult](body)
              log.info(s"scenariodetailsresult: ${conv.hostname}, ${conv.txn}")
              val conv2 = ScenarioDetailsResultWithMap(conv.hostname, conv.process, conv.model, conv.line, conv.diff, conv.scname, conv.result, conv.altxt, conv.currentstep, conv.snapshot, conv.txn, conv.ts, conv.details, Map.empty[String,String])
              system.actorSelection("/user/Master/JobActor") ! conv2
              complete("ok")
            case Some("scenariodetailsresultWithMap") =>
              val conv = read[ScenarioDetailsResultWithMap](body)
              log.info(s"scenariodetailsresultWithMap: ${conv.hostname}, ${conv.txn}")
              system.actorSelection("/user/Master/JobActor") ! conv
              complete("ok")
            case Some("tkincancel") =>
              val conv = read[RequestTkinCancel](body)
              log.info(s"tkincancel: ${conv.hostname}, ${conv.txn}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("releasemessage") =>
              val conv = read[RequestReleaseMsg](body)
              log.info(s"RequestReleaseMsg: ${conv.hostname}, ${conv.txn}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("serverrestart") =>
              val conv = read[RequestServerRestart](body)
              log.info(s"serverrestart: ${conv.hostname}, ${conv.txn}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("eqpdown") =>
              val conv = read[RequestEquipmentDown](body)
              log.info(s"eqpdown: ${conv.hostname}, ${conv.txn}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("prepmstart") =>
              val conv = read[PrePMStart](body)
              log.info(s"prepmstart: ${conv.hostname}, ${conv.pmcode}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("prepmcompl") =>
              val conv = read[PrePMComplete](body)
              log.info(s"prepmcompl: ${conv.hostname}, ${conv.pmcode}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("physicalpmstart") =>
              val conv = read[PhysicalPMStart](body)
              log.info(s"physicalpmstart: ${conv.hostname}, ${conv.pmcode}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("physicalpmcompl") =>
              val conv = read[PhysicalPMComplete](body)
              log.info(s"physicalpmcompl: ${conv.hostname}, ${conv.pmcode}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("postpmstart") =>
              val conv = read[PostPMStart](body)
              log.info(s"postpmstart: ${conv.hostname}, ${conv.pmcode}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("postpmcompl") =>
              val conv = read[PostPMComplete](body)
              log.info(s"postpmcompl: ${conv.hostname}, ${conv.pmcode}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("ssbaction") =>
              val conv = read[SSBAction](body)
              log.info(s"ssbaction: ${conv.hostname}, ${conv.smlid}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("acscall") =>
              val conv = read[ACSCall](body)
              log.info(s"acscall: ${conv.hostname}, ${conv.comments}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("loadportautojob") =>
              val conv = read[LoadPortAutoJob](body)
              log.info(s"loadportautojob: ${conv.hostname}, ${conv.portLists.mkString(",")}")
              onComplete((system.actorSelection("/user/Master/RVWorker") ? conv).mapTo[String]){
                case Success(value) => complete(value)
                case Failure(ex) => failWith(ex)
              }
            case Some("test") =>
              if(headerMap.get("type") != None){
                headerMap.get("type") match {
                  case Some("newdatafromlocal") =>
                    val conv = read[NewDataFromLocal](body)
                    log.info(s"test newdatafromlocal: ${conv.hostname}, ${conv.txn}")
                    complete("ok")
                  case Some("newdatafromlocalwithparams") =>
                    val conv = read[NewDataFromLocalWithParams](body)
                    log.info(s"test newdatafromlocalwithparams: ${conv.hostname}, ${conv.txn}")
                    complete("ok")
                  case Some("updatestatus") =>
                    val conv = read[AgentEvent](body)
                    log.info(s"test updatestatus: ${conv.hostname}, ${conv.txn}")
                    complete("ok")
                  case Some("sendemail") =>
                    val conv = read[RequestEmailTrans](body)
                    val exists = if(conv.snapshot != null && conv.snapshot != "") true else false
                    log.info(s"test sendemail: ${conv.hostname}, ${conv.ip}, ${conv.process}, ${conv.process}, ${conv.line}, ${conv.model}, ${conv.code}, ${conv.subcode}, ${exists}")
                    complete("ok")
                  case Some("scenarioresult") =>
                    val conv = read[ScenarioResult](body)
                    log.info(s"test scenarioresult: ${conv.hostname}, ${conv.txn}")
                    val conv2 = ScenarioDetailsResultWithMap(conv.hostname, conv.process, conv.model, conv.line, conv.diff, conv.scname, conv.result, conv.altxt, conv.currentstep, conv.snapshot, conv.txn, conv.ts, "", Map.empty[String,String])
                    complete("ok")
                  case Some("scenariodetailsresult") =>
                    val conv = read[ScenarioDetailsResult](body)
                    log.info(s"test scenariodetailsresult: ${conv.hostname}, ${conv.txn}")
                    val conv2 = ScenarioDetailsResultWithMap(conv.hostname, conv.process, conv.model, conv.line, conv.diff, conv.scname, conv.result, conv.altxt, conv.currentstep, conv.snapshot, conv.txn, conv.ts, conv.details, Map.empty[String,String])
                    complete("ok")
                  case Some("scenariodetailsresultWithMap") =>
                    val conv = read[ScenarioDetailsResultWithMap](body)
                    log.info(s"test scenariodetailsresultWithMap: ${conv.hostname}, ${conv.txn}")
                    complete("ok")
                  case Some("tkincancel") =>
                    val conv = read[RequestTkinCancel](body)
                    log.info(s"test tkincancel: ${conv.hostname}, ${conv.txn}")
                    complete("ok")
                  case Some("releasemessage") =>
                    val conv = read[RequestReleaseMsg](body)
                    log.info(s"test RequestReleaseMsg: ${conv.hostname}, ${conv.txn}")
                    complete("ok")
                  case Some("serverrestart") =>
                    val conv = read[RequestServerRestart](body)
                    log.info(s"test serverrestart: ${conv.hostname}, ${conv.txn}")
                    complete("ok")
                  case Some("eqpdown") =>
                    val conv = read[RequestEquipmentDown](body)
                    log.info(s"test eqpdown: ${conv.hostname}, ${conv.txn}")
                    complete("ok")
                  case Some("prepmstart") =>
                    val conv = read[PrePMStart](body)
                    log.info(s"test prepmstart: ${conv.hostname}, ${conv.pmcode}")
                    complete("ok")
                  case Some("prepmcompl") =>
                    val conv = read[PrePMComplete](body)
                    log.info(s"test prepmcompl: ${conv.hostname}, ${conv.pmcode}")
                    complete("ok")
                  case Some("physicalpmstart") =>
                    val conv = read[PhysicalPMStart](body)
                    log.info(s"test physicalpmstart: ${conv.hostname}, ${conv.pmcode}")
                    complete("ok")
                  case Some("physicalpmcompl") =>
                    val conv = read[PhysicalPMComplete](body)
                    log.info(s"test physicalpmcompl: ${conv.hostname}, ${conv.pmcode}")
                    complete("ok")
                  case Some("postpmstart") =>
                    val conv = read[PostPMStart](body)
                    log.info(s"test postpmstart: ${conv.hostname}, ${conv.pmcode}")
                    complete("ok")
                  case Some("postpmcompl") =>
                    val conv = read[PostPMComplete](body)
                    log.info(s"test postpmcompl: ${conv.hostname}, ${conv.pmcode}")
                    complete("ok")
                  case Some("ssbaction") =>
                    val conv = read[SSBAction](body)
                    log.info(s"test ssbaction: ${conv.hostname}, ${conv.smlid}")
                    complete("ok")
                  case Some("acscall") =>
                    val conv = read[ACSCall](body)
                    log.info(s"test acscall: ${conv.hostname}, ${conv.comments}")
                    complete("ok")
                  case Some("loadportautojob") =>
                    val conv = read[LoadPortAutoJob](body)
                    log.info(s"test loadportautojob: ${conv.hostname}, ${conv.portLists.mkString(",")}")
                    complete("ok")
                  case other =>
                    failWith(new Exception("invalid type headers"))
                }
              } else {
                failWith(new Exception("invalid type headers"))
              }
            case other =>
              failWith(new Exception("Invalid function headers"))
          }
        }
      }
    } ~
    path("EARS" / "Trigger") {
      post {
        (extractRequest & extractClientIP & entity(as[String])) { (request, ip, params) =>
          log.info(s"/EARS/Trigger request received: ${ip.toOption.map(_.getHostAddress).getOrElse("unknown")}")
          val headerMap = request.headers.map(h => (h.name(),h.value())).toMap
          if(headerMap.get("process") != None && headerMap.get("model") != None && headerMap.get("eqpid") != None && headerMap.get("line") != None
            && headerMap.get("code") != None  && headerMap.get("triggeredBy") != None){
            val curTS = System.currentTimeMillis()
            log.info(s"auto recovery data inserted: ${headerMap("eqpid")}, ${headerMap("code")}")
            //process: String, model: String, eqpid: String, line: String, ears_code: String, txn: Long, data: String
            system.actorSelection("/user/Master/JobActor") ! AutoRecoveryData(headerMap("process"), headerMap("model"), headerMap("eqpid"), headerMap("line"), headerMap("code"), curTS, params, headerMap("triggeredBy"))
            complete("ok")
          }else{
            failWith(new Exception("invalid http headers: eqpid, txn, code"))
          }
        }
      }
    } ~
    path("EARS" / "Trigger" / "Bulk") {
      post {
        (extractClientIP & entity(as[String])) { (ip, body) =>
          log.info(s"/EARS/Trigger/Bulk request received: ${ip.toOption.map(_.getHostAddress).getOrElse("unknown")}")
          system.actorSelection("/user/Master/JobActor") ! AutoRecoveryBulkData(body)
          complete("ok")
        }
      }
    } ~
    path("EARS" / "kill") {
      post {
        system.actorSelection("/user/Master") ! ShutDown()
        complete("ok")
      }
    }
  }
}

class HttpWorker extends Actor{
  import HttpWorker._

  override def preStart() : Unit = {
    Http().bindAndHandle( route, "0.0.0.0", ServiceConfig.HttpPort)
    log.info(s"HttpWorker(${self.path}) preStart")
  }

  override def postStop() : Unit = {
    Http().shutdownAllConnectionPools()
    log.info(s"HttpWorker(${self.path}) postStop")
  }

  def receive = {
    case _ =>
  }
}