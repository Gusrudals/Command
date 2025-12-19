package com.sec.eeg.ars.data

import java.io.FileReader
import java.util.Properties
import com.mongodb.client.MongoDatabase
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser
import org.slf4j.LoggerFactory
import scala.concurrent.duration._


object ServiceConfig {
  private val logger = LoggerFactory.getLogger(ServiceConfig.toString)
  val AkkaSystem = "EARSSystem"
  val serviceName = "CommandServer"
  val ZookeeperNodePath : String = "/EARS/CommandServer"
  var ZookeeperQuorum : String = ""
  var MyServiceAddress : String = ""
  var RedisSentinelConnection : String = _
  var RVResponseTimeOut : FiniteDuration = _
  var JobWorkerCount = 5
  var RVWorkerCount = 3
  var Version = "unknown"
  var KafkaBootstrapServers = ""
  var DatabaseTimeOut : FiniteDuration = 1 minutes
  val RecoveryAllowableTimeLimit: FiniteDuration = 10 minutes
  var RedisPingInterval : FiniteDuration = _
  var HttpSrvPublicUrl = ""
  var HttpPort : Int = 8080
  var VirtualPublicIpAddress = ""
  var HttpWebServerPublicPort : Int = 50005
  var HttpWebServerAddress = ""
  var MongoDBUrl = ""
  var database : MongoDatabase = _

  var TriggerControlWithDBInfo : Boolean = false
  var GrpcTimeout : FiniteDuration = 3 seconds
  var GrpcRetryCount = 3
  
  def load : Boolean = {
    val root_dir = System.getProperty("EEG_BASE")
    val parser = new JSONParser
    val confjson = parser.parse(new FileReader(root_dir + s"/conf/$serviceName/$serviceName.json")).asInstanceOf[JSONObject]

    try{
      VirtualPublicIpAddress = confjson.get("VirtualPublicIpAddress").asInstanceOf[String]
    }catch {
      case _: Throwable =>
        if(VirtualPublicIpAddress == null || VirtualPublicIpAddress.trim == ""){
          System.exit(1)
        }
    }

    try{
      KafkaBootstrapServers = confjson.get("KafkaBootstrapServers").asInstanceOf[String]
    }catch {
      case _: Throwable =>
        if(KafkaBootstrapServers == null || KafkaBootstrapServers.trim == ""){
          logger.error("KafkaBootstrapServers is null")
          System.exit(6)
        }
    }

    try{
      HttpWebServerAddress = confjson.get("HttpWebServerAddress").asInstanceOf[String]
    }catch {
      case _: Throwable =>
        if(HttpWebServerAddress == null || HttpWebServerAddress.trim == ""){
          System.exit(1)
        }
    }

    val _HttpWebServerPort = System.getenv("HTTP_WEB_SRV_PORT")
    if(_HttpWebServerPort == null || _HttpWebServerPort.trim == ""){
      System.exit(5)
    }
    HttpWebServerPublicPort = _HttpWebServerPort.toInt

    HttpSrvPublicUrl = s"$VirtualPublicIpAddress:$HttpWebServerPublicPort"
    
    logger.info(s"HttpSrvPublicUrl is $HttpSrvPublicUrl")

    try{
      ZookeeperQuorum = confjson.get("ZookeeperQuorum").asInstanceOf[String]
    }catch {
      case _: Throwable =>
        if(ZookeeperQuorum == null || ZookeeperQuorum.trim == ""){
          System.exit(4)
        }
    }

    val _HttpPort = System.getenv("HTTP_PORT")
    if(_HttpPort == null || _HttpPort.trim == ""){
      System.exit(5)
    }
    HttpPort = _HttpPort.toInt

    val _MyServiceAddress = System.getenv("HOST_IP")
    if(_MyServiceAddress == null || _MyServiceAddress.trim == ""){
      System.exit(5)
    }

    MyServiceAddress = s"${_MyServiceAddress}:$HttpPort"

    try{
      RedisSentinelConnection = confjson.get("RedisSentinelConnection").asInstanceOf[String]
    }catch {
      case _: Throwable =>
        if(RedisSentinelConnection == null || RedisSentinelConnection.trim == ""){
          System.exit(6)
        }
    }

    try{
      _DatabaseTimeOut = Duration(confjson.get("DatabaseTimeOut").asInstanceOf[String])
      if(_DatabaseTimeOut != null){
        DatabaseTimeOut = FiniteDuration(_DatabaseTimeOut.length, _DatabaseTimeOut.unit)
      }
    }
    catch {
      case _: Throwable => DatabaseTimeOut = 1 minutes
    }

    try{
      _RedisPingInterval = Duration(confjson.get("RedisPingInterval").asInstanceOf[String])
      if(_RedisPingInterval != null){
        RedisPingInterval = FiniteDuration(_RedisPingInterval.length, _RedisPingInterval.unit)
      }
    }
    catch {
      case _: Throwable => RedisPingInterval = 20 seconds
    }

    _RVResponseTimeOut = Duration(confjson.get("RVResponseTimeOut").asInstanceOf[String])
    RVResponseTimeOut = FiniteDuration(_RVResponseTimeOut.length, _RVResponseTimeOut.unit)

    try{ JobWorkerCount = confjson.get("JobWorkerCount").asInstanceOf[Long].toInt
    if(JobWorkerCount == 0) JobWorkerCount = 5}
    catch { case _: Throwable => JobWorkerCount = 5}

    try{ RVWorkerCount = confjson.get("RVWorkerCount").asInstanceOf[Long].toInt
    if(RVWorkerCount == 0) RVWorkerCount = 5}
    catch { case _: Throwable => RVWorkerCount = 5}

    val IP_Port = MyServiceAddress.split(":")
    if(IP_Port.size != 2 || IP_Port(0) == "" || IP_Port(1) == "")
      throw new Exception("Invalid Service Address")
    
    MongoDBUrl = confjson.get("MongoDBUrl").asInstanceOf[String]
    if(MongoDBUrl == null || MongoDBUrl.trim == "") {
      throw new Exception("Invalid MongoDB Url")
    }

    TriggerControlWithDBInfo = confjson.get("TriggerControlWithDBInfo").asInstanceOf[Boolean]

    try{
      _grpcTimeout = Duration(confjson.get("GrpcTimeout").asInstanceOf[String])
      if(_grpcTimeout != null){
        GrpcTimeout = FiniteDuration(_grpcTimeout.length, _grpcTimeout.unit)
      }
    }
    catch {
      case _: Throwable => GrpcTimeout = 3 seconds
    }
      
    try{
      GrpcRetryCount = confjson.get("GrpcRetryCount").asInstanceOf[Long].toInt
      if(GrpcRetryCount == 0) { GrpcRetryCount = 3 }
    }
    catch {
      case _: Throwable => GrpcRetryCount = 3
    }

    getVersion()

    logger.info("Service Config loaded")

    true
  }

  def getVersion() = {
    val is = this.getClass.getClassLoader.getResourceAsStream("META-INF/maven/com.sec.eeg.ars/CommandServer/pom.properties")

    if (is != null) {
      val p => new Properties()
      p.load(is)
      Version = p.getProperty("version", "")
    }
    if(Version == null || Version == ""){
      val aPackage = getClass.getPackage
      if(aPackage != null){
        Version = aPackage.getImplementationVersion
        if(Version == null || Version == ""){
          Version = aPackage.getSpecificationVersion
        }
      }
    }
    if(Version == null)
      Version = "unknown"
    logger.info(s"program release version: $Version")
  }
}