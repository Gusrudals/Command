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
  import context.dispatcher //RedisHGETData null 처리
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
    

}