package com.sec.eeg.ars.remote

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.TimeUnit

import com.lambdaworks.redis.RedisClient
import com.sec.eeg.ars.data.ServiceConfig
import com.sec.eeg.ars.data.agentendpointkeeper.{AgentendpointkeeperGrpc, JasonStringRequest, NoParams, ResponseResult}

import io.grpc._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

//GRPCAgentStatus -> AgentStatus
object AgentStatus extends Enumeration {
  val CommandSuccess, CommandFailed, NotOnline = Value
}


object GPRCConnector {
  private val log = LoggerFactory.getLogger(GRPCConnector.toString)
  val maxTryCount = ServiceConfig.GrpcRetryCount

  val redisClient = {
    if(ServiceConfig.RedisSentinelConnection.exist(ch => ch == "#"))
      RedisClient.create(s"redis-sentinel://visuallove@${ServiceConfig.RedisSentinelConnection}")
    else
      RedisClient.create(s"redis://visuallove@${ServiceConfig.RedisSentinelConnection}")
  }

  def sendScenarioCommandToAgent(process: String, model: String, eqpId:String, triggerInfo: String): AgentStatus.Value = {
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
        log.warn(s"${process}, ${model}, ${eqpId} send run scenario command - agent is not online")
        return AgentStatus.NotOnline
      }

      val grpcStatus = retry(maxTryCount)(sendGrpc(agentInfo, process, model, eqpId, triggerInfo))
      if (grpcStatus) {
        results = AgentStatus.CommandSuccess
      }
    }
    catch {
      case ex: Throwable =>
        log.error(s"${process}, ${model}, ${eqpId} send run scenario command - failed ${ex.getStackTraceString}")
        results = AgentStatus.CommandFailed
    }
    finally {
      redisConnection.close()
    }

    results
  }

  def sendGrpc(agentInfo: Array[String], process: String, model: String, eqpid: String, triggerInfo: String): Boolean = {
    var channel: ManagedChannel = null
    try {
      val port = agentInfo(1)
      val ipAddress = agentInfo(2)
      val innerIpAddress = agentInfo(3)

      //grpc channel 구성
      if (innerIpAddress != "_") {
        channel = ManagedChannelBuilder.forAddress(innerIpAddress, port.toInt).proxyDetector(new ProxyDetector {
          val proxyAddress = new InetSocketAddress(ipAddress, 7184)

          override def proxyFor(targetServerAddress: SocketAddress): ProxiedSocketAddress = {
            return HttpConnectProxiedSocketAddress.newBuilder().setTargetAddress(targetServerAddress.asInstanceOf[InetSocketAddress])
              .setProxyAddress(proxyAddress).build()
          }
        }).usePlaintext().build()
      }
      else {
        channel = ManagedChannelBuilder.forAddress(ipAddress, agentInfo(1).toInt).usePlaintext().build()
      }

      //send message
      val stub = AgentendpointkeeperGrpc.newFutureStub(channel).withDeadlineAfter(ServiceConfig.GrpcTimeout.length, ServiceConfig.GrpcTimeout.unit)
      val future = stub.runScenario(JasonStringRequest.newBuilder().setJson(s"${triggerInfo}").build())

      val response = future.get(ServiceConfig.GrpcTimeout.length, ServiceConfig.GrpcTimeout.unit)
      if (response.getSuccess) {
        log.info(s"${process}, ${model}, ${eqpid} send run scenario command - success: ${ipAddress}, ${innerIpAddress}")
        true
      }
      else {
        log.warn(s"${process}, ${model}, ${eqpid} send run scenario command - failed: ${ipAddress}, ${innerIpAddress}")
        false
      }
    } catch {
      case ex: Throwable =>
        log.error(s"${process}, ${model}, ${eqpid} send run scenario command - failed: ${ex.getStackTraceString}")
        throw ex
    } finally {
      channel.shutdownNow()
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