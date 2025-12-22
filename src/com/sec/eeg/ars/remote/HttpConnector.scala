package com.sec.eeg.ars.remote

import com.lambdaworks.redis.RedisClient
import com.mashape.unirest.http.Unirest
import com.sec.eeg.ars.data._
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

//Http request 용
object Function extends Enumeration {
  val UpdateScenario, UpdateScript, RunScenario, RunScript, runRTMScript, runtRTMPopup, runRTMEvent, HealthCheck, BackupData, RecoveryData = Value
}

case class HTTPRETURN(result: String, message: String)



object HttpConnector {
  implicit val formats = DefaultFormats
  private val log = LoggerFactory.getLogger(HttpConnector.toString)


  val redisClient = {
    if(ServiceConfig.RedisSentinelConnection.exist(ch => ch == "#"))
      RedisClient.create(s"redis-sentinel://visuallove@${ServiceConfig.RedisSentinelConnection}")
    else
      RedisClient.create(s"redis://visuallove@${ServiceConfig.RedisSentinelConnection}")
  }

  val maxTryCount = 3 //val maxTryCount = ServiceConfig.GrpcRetryCount

  def SendRecoveryEmail(req: RecoveryEmailHttpDataFormat) : (Boolean, String) = {
    var returnVal : (Boolean, String) = null
    try {
      val jsonString = JsonDataFormat.toJson(req)
      log.info(s"SendRecoveryEmail : ${req.scname}, http://${ServiceConfig.HttpWebServerAddress}/RecoveryEmailNotify")

      val response = Unirest.post(s"http://${ServiceConfig.HttpWebServerAddress}/RecoveryEmailNotify")
                      .header("accept", "application/json")
                        .body(jsonString).asJson()

      if(response.getStatus != 200){
        returnVal = (false, response.getStatusText)
      }
      else{
        val _ret = response.getBody.getObject.getString("result")
        if (_ret == "Success") {
          returnVal = (true, "")
        }
        else {
          returnVal = (false, response.getBody.getObject.getString("message"))
        }
      }
    }
    catch {
      case ex: Throwable => log.error(s"Exception: ${ex.getStackTraceString}")
        returnVal = (false, ex.getMessage)
    }

    return returnVal
  }

  def SendEmailNotification(req: EmailHttpDataFormat) : (Boolean, String) = {
    var returnVal : (Boolean, String) = null
    try {
      val jsonString = JsonDataFormat.toJson(req)
      log.info(s"SendEmailNotification : ${req.code}-${req.subcode}")

      val response = Unirest.post(s"http://${ServiceConfig.HttpWebServerAddress}/EmailNotify")
                      .header("accept", "application/json")
                        .body(jsonString).asJson()

      if(response.getStatus != 200){
        returnVal = (false, response.getStatusText)
      }
      else{
        val _ret = response.getBody.getObject.getString("result")
        if (_ret == "Success") {
          returnVal = (true, "")
        }
        else {
          returnVal = (false, response.getBody.getObject.getString("message"))
        }
      }
    }
    catch {
      case ex: Throwable => log.error(s"Exception: ${ex.getStackTraceString}")
        returnVal = (false, ex.getMessage)
    }

    return returnVal
  }

  //send http to agent
  def sendCommandToEquip(func: Function.Value, process: String, model: String, eqpId:String, jsonString: String) : AgentStatus.Value = {
    val redisConnection = redisClient.connect()
    var results : AgentStatus.Value = AgentStatus.CommandFailed
    try {
      if (ServiceConfig.TriggerControlWithDBInfo) {
        if(!MongoDBConnector.isEquipTriggerEnable(eqpId)) {
          return AgentStatus.NotOnline
        }
      }

      //agent 기준정보 확인
      val agentMetaInfo = redisConnection.sync().hget(s"AgentMetaInfo:${process}-${model}", eqpId)
      if (agentMetaInfo == null || agentMetaInfo == "" || agentMetaInfo.split(":").size < 4) { //AgentMetaInfo size 4 -> 5 (6.8.9.19 이상은 size 5)
        log.warn(s"${process}, ${model}, ${eqpId} is not online - AgentMetaInfo is not validate in redis")
        return AgentStatus.NotOnline
      }

      //agent 동작 상태 확인
      val agentInfo = agentMetaInfo.split(":")
      val agent = redisConnection.sync().get(s"AgentRunning:${process}-${model}-${eqpId}")
      if (agent == null) {
        log.warn(s"${process}, ${model}, ${eqpId} send ${func} command - agent is not online")
        return AgentStatus.NotOnline
      }

      val httpStatus = retry(maxTryCount)(sendHttp(agentInfo, eqpId, func, jsonString))
      if (httpStatus) {
        results = AgentStatus.CommandSuccess
      }
    }
    catch {
      case ex: Throwable =>
        log.error(s"${process}, ${model}, ${eqpId} send ${func} command - failed ${ex.getStackTraceString}")
        results = AgentStatus.CommandFailed
    }
    finally {
      redisConnection.close()
    }

    results
  }

  def sendHttp(agentMetaInfo: Array[String], eqpId:String, func: Function.Value, request: String) = {
    var httpClient : CloseableHttpClient = null
    log.info(s"sendHttp, eqp:${eqpId}, func:${func}")

    try {
      val port = agentInfo(1)
      val ipAddress = agentInfo(2)
      val innerIpAddress = agentInfo(3)
      var requestIp = ipAddress

      val _requestConfig = RequestConfig.custom().setSocketTimeout(5000).setConnectionTimeout(5000)
      if (innerIpAddress != "_") {
        _requestConfig.setProxy(new HttpHost(ipAddress, 7184))
        requestIp = innerIpAddress
      }
      val requestConfig = _requestConfig.build()
      httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build()

      val post = new HttpPost(s"http://${requestIp}:${port}/Agent/Action")
      post.setEntity(new StringEntity(request, "UTF-8"))
      post.setHeader("func", func.toString)

      var response: CloseableHttpResponse = null
      response = httpClient.execute(post)
      val body = EntityUtils.toString(response.getEntity)
      log.info(s"Response code from Agent: ${response.getStatusLine,getStatusCode}, ${body}")
      if(response.getStatusLine.getStatusCode == 200){
        true
      }else{
        false
      }

    } catch {
      case ex : Throwable =>
        log.warn(s"$func failed:", ex)
        throw ex
    } finally {
      if (httpClient != null)
        httpClient.close()
    }
  }

  @annotation.tailrec
  private def retry[T](n: Int)(fn: => T): T = {
    Try {
      fn
    } match {
      case Success(x) => x
      case _ if n > 1 =>
        log.debug("sleep for retry")
        Thread.sleep(1000)
        log.info(s"retry ${maxTryCount - n + 1}")
        retry(n - 1)(fn)
      case Failure(e) =>
        log.info("retry failure")
        throw e
    }
  }
}