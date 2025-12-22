package com.sec.eeg.ars.actor

import java.io.File
import java.util.concurrent.TimeUnit
import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, PoisonPill, Props, Terminated}
import akka.pattern.{Backoff, BackoffSupervisor, gracefulStop}
import akka.routing.SmallestMailboxPool
import com.mongodb.client.MongoClients
import com.sec.eeg.ars.data.ServiceConfig.{MongoDBUrl, database}
import com.sec.eeg.ars.data.{MultiLangMgr, ServiceConfig}
import com.tibco.tibrv.Tibrv
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListener}
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.CloseableUtils
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object Master {
  private val logger = LoggerFactory.getLogger(Master.toString)
  case class ShutDown()
  var system : ActorSystem = _
  var master : ActorRef = _
  var conf : Config = _
  val stoplock = new Object()

  init

  def init = {
    try{
      val root_dir=System.getProperty("EEG_BASE")
      val akkaApplicationConf = new File(root_dir + s"/conf/${ServiceConfig.serviceName}/application.conf")
      conf = ConfigFactory.parseFile(akkaApplicationConf)
    }
    catch {
      case ex : Exception =>
    }
  }

  def MakeActor(): Unit ={
    if(master == null){
      if(ServiceConfig.load == false)
        throw new Exception("Service Config loading is failed")
      system = ActorSystem.create(ServiceConfig.AkkaSystem, ConfigFactory.load(conf))
      master = system.actorOf(Props[Master],"Master")
    }
  }

  def StopForTermination(): Unit ={
    if(master != null){
      try{
        val stopped : Future[Boolean] = gracefulStop(master, 20 seconds, ShutDown())
        Await.result(stopped, 30 seconds)
      }catch{
        case e: akka.pattern.AskTimeoutException =>
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 1)
    {
      return
    }

    try{
      if(args(0) == "start")
      {
        MakeActor()
        logger.info("CommandServer started")
      }
      else if(args(0) == "stop") {
        stoplock.synchronized{
          StopForTermination()
          logger.info("CommandServer stopepd")
        }
      }
    }
    catch{
      case ex: Exception => logger.error("program execution failed: ${ex}")
    }
  }
}


class Master extends  Actor {
  private val log = LoggerFactory.getLogger(classOf[Master])
  import Master._
  var childsCnt : Int = 0
  var selectorService: LeaderSelector = null
  var zkCli : CuratorFramework = null

  private def makeHttpEndpoint() = {
    val HttpWorkerProps = Props(classOf[HttpWorker])

    val HttpWorkerSupervisor = BackoffSupervisor.props(
      Backoff.onFailure(HttpWorkerProps, "HttpWorker", Duration.create(3, TimeUnit.SECONDS), Duration.create(30, TimeUnit.SECONDS), 0.2
      ).withSupervisorStrategy(
        OneForOneStrategy() {
          case ex: Throwable                =>
            log.info(s"Master supervisorStrategy: ${ex}")
            Restart
        })
    )

    createChildActor(context.actorOf(HttpWorkerSupervisor, "HttpWorker"))
  }


  private def makeChild() = {

    val RedisActorProps = Props(classOf[RedisActor])

    val JobActorProps = Props(classOf[JobActor], conf)
    val RVWorkerProps = Props(classOf[RVWorker], conf)

    val RedisActorSupervisor = BackoffSupervisor.props(
      Backoff.onFailure(HttpWorkerProps, "RedisActor", Duration.create(3, TimeUnit.SECONDS), Duration.create(30, TimeUnit.SECONDS), 0.2
      ).withSupervisorStrategy(
        OneForOneStrategy() {
          case ex: Throwable                =>
            log.info(s"Master supervisorStrategy: ${ex}")
            Restart
        })
    )

    val JobActorSupervisor = BackoffSupervisor.props(
      Backoff.onFailure(HttpWorkerProps, "JobActor", Duration.create(3, TimeUnit.SECONDS), Duration.create(30, TimeUnit.SECONDS), 0.2
      ).withSupervisorStrategy(
        OneForOneStrategy() {
          case ex: Throwable                =>
            log.info(s"Master supervisorStrategy: ${ex}")
            Restart
        })
    )

    val RVWorkerSupervisor = BackoffSupervisor.props(
      Backoff.onFailure(HttpWorkerProps, "RVWorker", Duration.create(3, TimeUnit.SECONDS), Duration.create(30, TimeUnit.SECONDS), 0.2
      ).withSupervisorStrategy(
        OneForOneStrategy() {
          case ex: Throwable                =>
            log.info(s"Master supervisorStrategy: ${ex}")
            Restart
        })
    )

    createChildActor(context.actorOf(RedisActorSupervisor, "RedisActor"))
    createChildActor(context.actorOf(SmallestMailboxPool(ServiceConfig.JobWorkerCount).props(JobActorSupervisor), "JobActor"))
    createChildActor(context.actorOf(SmallestMailboxPool(ServiceConfig.RVWorkerCount).props(RVWorkerSupervisor), "RVWorker"))
    createChildActor(context.actorOf(MessageProducer.props(), "MessageProducer"))

    makeHttpEndpoint()

    database = MongoClients.create(MongoDBUrl).getDatabase("EARS")
    log.info("Master makeChild")
  }


  private def createChildActor(childActor: ActorRef): ActorRef ={
    childsCnt += 1
    context.watch(childActor)
  }

  override def preStart() : Unit = {

    MultiLangMgr.init

    if(System.getProperty("MODE") != "DEBUG")
      Tibrv.open(Tibrv.IMPL_NATIVE)

    makeChild()

    val selectorListener = new LeaderSelectorListener()
    {
      override def takeLeadership(curatorFramwork: CuratorFramework): Unit = {
        if(master != null){
          try{
            log.info("set this command service as a leader - Pubsub Redis for delayed scenario")
            curatorFramwork.setData().forPath(s"${ServiceConfig.ZookeeperNodePath}/leader", ServiceConfig.MyServiceAddress.getBytes)
            context.actorSelection("RedisActor") ! ListenMessage()

            stoplock.synchronized{
              stoplock.wait()
            }
          }
          catch{
            case ex: InterruptedException => log.info("this command service is not leader")
            case ex: Throwable => log.error(s"Critical error has occurred:")
              System.exit(1)
          }
        }
      }
      override def stateChanged(client: CuratorFramework, s: ConnectionState): Unit = {
        if(s == ConnectionState.LOST){
          client.delete().forPath(s"${ServiceConfig.ZookeeperNodePath}/daemons/${ServiceConfig.MyServiceAddress}")
          CloseableUtils.closeQuietly(selectorService)
          log.info("selectorService is closed")
        }
      }
    }

    zkCli = CuratorFrameworkFactory.newClient(ServiceConfig.ZookeeperQuorum, new ExponentialBackoffRetry(1000, 3))
    selectorService = new LeaderSelector(zkCli, s"${ServiceConfig.ZookeeperNodePath}/leader", selectorListener)
    selectorService.autoRequeue()
    zkCli.start()
    zkCli.getZookeeperClient.blockUntilConnectedOrTimedOut()
    if(zkCli.getState != CuratorFrameworkState.STARTED){
      throw new Exception("Zk startup timed out")
    }

    //zookeeper parent node 확인/생성
    val zkBasePath = s"${ServiceConfig.ZookeeperNodePath}/daemons"
    try{
      zkCli.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(zkBasePath) //parent
    } catch {
      case _: NodeExistsException => //이미 있으면 무시
    }
    //zookeeper node 존재 확인 후 등록
    val zkNodePath = s"${zkBasePath}/${ServiceConfig.MyServiceAddress}"
    val zkNodeData = ServiceConfig.Version.getBytes
    try {
      zkCli.create().withMode(CreateMode.EPHEMERAL).forPath(zkNodePath,zkNodeData)
    } catch {
      case _: NodeExistsException =>
        log.info("zookeeper node already exist")
        // Curator 내부의 ZooKeeper 인스턴스에서 현재 세션 ID를 꺼내기
        val stat = zkCli.checkExists().forPath(zkNodePath)
        val zk   = zkCli.getZookeeperClient.getZooKeeper
        val mySessionId = zk.getSessionId
        if (stat.getEphemeralOwner == mySessionId) {// 같은 세션이 만든 노드 → 데이터만 업데이트
          log.info(s"same sessionId:${mySessionId}. update data:${zkNodeData}")
          zkCli.setData().forPath(zkNodePath, zkNodeData)
        } else { //  다른 세션(이전 인스턴스)이 만든 노드 → 삭제 후 재생성
          try{
            zkCli.delete().guaranteed().forPath(zkNodePath)
            log.info(s"zooKeeper node deleted successfully")
          }catch {
            case _: NodeExistsException => //삭제 실패 = 이미 삭제되었음
              log.info(s"zooKeeper node is already deleted")
          }
          zkCli.create().withMode(CreateMode.EPHEMERAL).forPath(zkNodePath, zkNodeData)
        }
    }
    selectorService.start()

    log.info("Master preStart")
  }

  override def postStop() : Unit = {
    if(System.getProperty("MODE") != "DEBUG")
      Tibrv.close()


    log.info("Master postStop")
  }

  def receive = {
    case ShutDown() =>

      if(zkCli != null){

        if(selectorService != null)
          selectorService.close()
        
        zkCli.delete().forPath(s"${ServiceConfig.ZookeeperNodePath}/daemons/${ServiceConfig.MyServiceAddress}")

        CloseableUtils.closeQuietly(zkCli)
      }

      val cnt = context.children.size
      if(cnt <= 0){
        context stop self
        system.terminate()
      }
      else{
        context.children.foreach( ch => {
          ch ! PoisonPill
        })
        context.become(shuttingDown)
      }
    case _ =>
  }

  def shuttingDown: Receive = {
    case Terminated(actor) =>
      log.info(s"Actor terminated: ${actor}")
      context.unwatch(actor)
      childsCnt -= 1
      log.debug(s"childCnt: ${childCnt}")
      if(childCnt <= 0){
        context stop self
        system.terminate()
      }
  }
}
