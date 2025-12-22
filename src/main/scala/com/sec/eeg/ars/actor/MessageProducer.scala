
import com.sec.eeg.ars.actor

import java.util.Properties
import akka.actor.{Actor, Props}
import com.fasterxml.jackson.databind.ObjectMapper
import com.sec.eeg.ars.data.ServiceConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.time.format.DateTimeFormatter
import org.json4s._
import org.slf4j.LoggerFactory

import java.time.{ZoneId, ZonedDateTime}
import scala.collection.JavaConverters._

case class SendMessageToStatusTopic(process: String, model: String, eqpid: String, ears_code: String, txn: Long, status: String, params: Map[String, Any])

case class SendMessageToResultTopic(process: String, model: String, eqpid: String, ears_code: String, txn: Long, status: String, params: Map[String, Any])

object MessageProducer {
  def props(): Props = Props(new MessageProducer())
}

class MessageProducer extends Actor = {
  private val log = LoggerFactory.getLogger(classOf[MessageProducer])
  implicit val formats: Formats = DefaultFormats
  private val objectMapper = new ObjectMapper()

  private var kafkaProducer : Option[KafkaProducer[String, String]] = None

  def initialize(): Unit = {
    try {
      val properties = new Properties()
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceConfig.KafkaBootstrapServers)
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      properties.put(ProducerConfig.ACKS_CONFIG, "all")
      properties.put(ProducerConfig.RETRIES_CONFIG, "0")
      properties.put(ProducerConfig.LINGER_MS_CONFIG, "1")
      properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432")

      kafkaProducer = Some(new KafkaProducer[String, String](properties))
    } catch {
      case ex: Throwable =>
        log.error("kafka producer initialize failed", ex)
    }
  }

  override def prStart(): Unit = {
    initialize()
    log.info("MessageProducer preStart")
  }

  override def postStop(): Unit = {
    kafkaProducer.foreach{ producer =>
      producer.cloase()
      kafkaProducer = None
      log.info(s"kafka producer close")
    }
    log.info("MessageProducer postStop")
  }

  override def receive: Receive = {
    case SendMessageToStatusTopic(process, model, eqpid, ears_code, txn, status, params) =>
      try {
        kafkaProducer match {
          case Some(producer) =>
            val now = ZonedDateTime.now(ZoneId.systemDefault())
            var data: Map[String, Any] = Map("PROCESS" -> process, "MODEL" -> model,"EQPID" -> eqpid, "SCENARIO_NAME" -> ears_code, "TXN" -> txn, "STATUS" -> status, "CREATE_DATE" -> now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))

            if (params.nonEmpty) {
              data = data ++ Map("PARAMS" -> params.asJava)
            }
            val jsonString = objectMapper.writeValueAsString(data.asJava)
            val record = new ProducerRecord ("cmd_scenario_status", eqpid, jsonString)

            producer.send(record)
          case None =>
            log.error("kafka producer is not initialized")
        }
      } catch {
        case ex: Throwable =>
          log.error("SendMessage Failed", ex)
      }

    case SendMessageToResultTopic(process, model, eqpid, ears_code, txn, status, params) =>
      try {
        kafkaProducer match {
          case Some(producer) =>
            val now = ZonedDateTime.now(ZoneId.systemDefault())
            var data = Map("PROCESS" -> process, "MODEL" -> model,"EQPID" -> eqpid, "SCENARIO_NAME" -> ears_code, "TXN" -> txn, "STATUS" -> status, "CREATE_DATE" -> now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME), "PARAMS" -> params)
            val jsonString = objectMapper.writeValueAsString(data.asJava)
            val record = new ProducerRecord ("cmd_scenario_result", eqpid, jsonString)

            producer.send(record)
          case None =>
            log.error("kafka producer is not initialized")
        }
      } catch {
        case ex: Throwable =>
          log.error("SendMessage Failed", ex)
      }

    case _ =>
  }
}