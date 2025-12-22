package com.sec.eeg.ars.actor

import akka.actor.Actor
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.api.{StatefulConnection, StatefulRedisConnection}
import com.lambdaworks.redis.pubsub.{RedisPubSubListener, StatefulRedisPubSubConnection}
import com.sec.eeg.ars.data.{JsonDataFormat, ServiceConfig}
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

case class RedisPubMessage(key: String, msg: String)
case class RedisSetData(key: String, data: String)
case class RedisSetExData(Key: String, data: String, interval: Long)
case class RedisHGetData(key:String, field: String)
case class RedisGetDataPM(key: String)
case class RedisSetDataPM(key: String, data: String, interval: Long)
case class ListenMessage()

class RedisActor extends Actor {
  private val log = LoggerFactory.getLogger(classOf[RedisActor])
  import scala.concurrent.ExecutionContext.Implicit.global

  case class Ping()

  var redisPubSubForNikon : StatefulRedisPubSubConnection[String,String] = null
  var redisClient : com.lambdaworks.redis.RedisClient = _
  var redisConnection : StatefulRedisConnection[String,String] = _
  var redisConnectionTimeDelay : StatefulRedisConnection[String,String] = _
  var redisPubSubConnectionTimeDelay : StatefulRedisPubSubConnection[String,String] = _
  val scheduled_start_scenario = "__keyevent@7__:expired"
  val scheduled_start_scenario_regex: Regex = scheduled_start_scenario.r
  val leaderListener: RedisPubSubListener[String, String] = new RedisPubSubListener[String,String]{
    override def message(k: String, v: String): Unit = {}

    override def message(p: String, ch: String, m: String): Unit = {
      ch match {
        case scheduled_start_scenario_regex() =>
          val pattern_deferred = "deferred@([a-zA-Z_\\-0-9\\.]+)@([a-zA-Z_\\-0-9\\.]+)@([a-zA-Z_\\-0-9\\.]+)@([a-zA-Z_\\-0-9\\.]+)@([0-9]+)@([0-9]+)".r
          val pattern_deferred_with_scen = "deferred@([a-zA-Z_\\-0-9\\.]+)@([a-zA-Z_\\-0-9\\.]+)@([a-zA-Z_\\-0-9\\.]+)@([a-zA-Z_\\-0-9\\.]+)@([0-9]+)@([0-9]+)@([a-zA-Z_\\-0-9\\.]+)".r
          try{
            m match {
              case pattern_deferred(process,model,eqpid,line,txn,deferred_time) =>
                log.info(s"A deferred realtime scenario occurred: $m")
                context.actorSelection("/user/Master/JobActor") ! AutoRecoveryData_Scheduled(process,model,eqpid,line,txn.toLong,deferred_time.toLong, "")
              case pattern_deferred_with_scen(process,model,eqpid,line,txn,deferred_time,scname) =>
                log.info(s"A deferred realtime scenario occurred: $m")
                context.actorSelection("/user/Master/JobActor") ! AutoRecoveryData_Scheduled(process, model, eqpid, line, txn.toLong, deferred_time.toLong, scname)
              case _ => // log.warn(s"Invalid deferred start scenario: ${m}")
            }
          }
          catch{
            case ex : Throwable => log.error(s"scheduled_start_scenario_regex exception: $ex")
          }

        case _ =>  log.warn("Received data is not match")
      }
    }

    override def punsubscribed(k: String, l: Long): Unit = {}

    override def subscribed(k: String, l: Long): Unit = {}

    override def unsubscribed(k: String, l: Long): Unit = {}

    override def psubscribed(k: String, l: Long): Unit = {}
  }


  def registRedisListener(): Unit = {
    log.info("redis PubSub Channel Open")
    redisPubSubConnectionTimeDelay = redisClient.connectPubSub()
    redisPubSubConnectionTimeDelay.sync().select(7)
    redisPubSubConnectionTimeDelay.sync().clientSetname("CommandServer")
    redisPubSubConnectionTimeDelay.addListener(leaderListener)
    redisPubSubConnectionTimeDelay.sync()psubscribe(scheduled_start_scenario)
    log.info("redis channel is successfully Opened")
  }

  def reconnetListener(): Unit = {
    try {
      redisPubSubConnectionTimeDelay.removeListener(leaderListener)
      redisPubSubConnectionTimeDelay.close()
      registRedisListener()
      log.info("reconnet PubSub Channel")
    }
    catch{
      case ex: Exception => log.warn(s"pubsub shutdown failed: $ex")
    }
  }

  //StatefulRedisConnection, StatefulRedisPubSubConnection 모두 처리하도록 수정. ConnectionName 추가
  def isConnected(connection: StatefulConnection[String, String], connectionName: String): Unit = {
    try {
      log.debug("RedisConnection check")
      connection match {
        case redisConn: StatefulRedisConnection[String, String] =>
          redisConn.sync().ping()
        case pubSubConn: StatefulRedisPubSubConnection[String, String] =>
          pubSubConn.sync().ping()
        case _ =>
          log.warn("Unsupported connection type")
      }
    } catch {
      case _: Throwable =>
        log.error(s"Exception occurred while checking Redis connection: $connectionName")
    } 
  }

  private def shutdown(): Unit = {
    try{
      if(redisClient != null){
        redisClient.shutdown()
      }
      log.info("RedisClient shutdown")
    }
    catch {
      case ex: Exception => log.warn(s"LettuceRedisClient shutdown failed: $ex")
    }
  }

  override preStart() : Unit = {
    if(ServiceConfig.RedisSentinelConnection.exist(ch => ch == "#")){
      RedisClient.create(s"redis-sentinel://visuallove@${ServiceConfig.RedisSentinelConnection}")
    }
    else{
      RedisClient.create(s"redis://visuallove@${ServiceConfig.RedisSentinelConnection}")
    }

    redisConnection = redisClient.connect()
    redisConnectionTimeDelay = redisClient.connect()
    redisConnectionTimeDelay.sync().select(7)

    redisPubSubForNikon = redisClient.connectPubSub()

    if(ServiceConfig.RedisPingInterval != null) {
      context.system.scheduler.scheduleOnce(ServiceConfig.RedisPingInterval, self, Ping())
    }

    log.info("RedisActor preStart")
  }

  override def postStop() : Unit = {
    shutdown()
    log.info("RedisActor postStop")
  }

  override def receive : Receive = {
    case ListenMessage() =>
      registRedisListener()
    case Ping() =>
      isConnected(redisConnection, "redisConnection")
      isConnected(redisConnectionTimeDelay, "redisConnectionTimeDelay")
      isConnected(redisPubSubForNikon, "redisPubSubForNikon")
      if(redisPubSubConnectionTimeDelay != null){
        val pubSubClient: Seq[String] = redisConnectionTimeDelay.sync().clientList().linesIterator.filter(s => s.contains("name=CommandServer")).toSeq
        if(pubSubClient.size < 1) {
          log.error("I'm a leader. But Redis Connection is disconnected. Try reconnect PubSub Channel.")
          reconnetListener()
        }
      }
      context.system.scheduler.scheduleOnce(ServiceConfig.RedisPingInterval, self, Ping())
    case RedisPubMessage(key,msg) => //grpc 사용 불가 설비용. 기존 redis 활용
      val ret = redisPubSubForNikon.sync().publish(key,msg)
      log.info(s"Redis Publish Message: ${key},${msg},${ret}")
    case RedisSetData(key,data) =>
      redisConnection.sync().set(key,data)
      log.info(s"Redis Set Message: $key,$data")
    case RedisSetExData(key,data,ttl) =>
      redisConnectionTimeDelay.sync().setex(key,ttl,data)
      log.info(s"Redis SetEx Message: $key,$data,$ttl")
    case RedisHGetData(key,field) =>
      try{
        val ret = redisConnection.sync().hget(key,field)
        val scenario = JsonDataFormat.getScenario(ret)
        log.info(s"Redis hget : $key,$field")
        sender() ! scenario
      }
      catch {
        case ex: Throwable => log.error(s"RedisHGetData Fail key-$key,field-$field : ${ex}")
      }
    case RedisGetDataPM(key) =>
      try{
        val ret = redisConnection.sync().get(key)
        log.info(s"Redis get : $key,$ret")
        if(ret == null){
          sender() ! ""
        }else{
          sender() ! ret
        }
      }
      catch {
        case ex: Throwable => log.error(s"RedisGetDataPM Fail key-$key : ${ex}")
      }
    case RedisSetDataPM(key,data,ttl) =>
      redisConnection.sync().setex(key, ttl, data)
      log.info(s"Redis Set Data PM Message: $key,$data,$ttl")
    case _ =>
  }
}