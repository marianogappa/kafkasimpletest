package simplekafkatest

import java.util.Properties

import kafka.common._
import kafka.message._
import kafka.producer._
import kafka.consumer._
import kafka.serializer._
import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable
import kafka.utils.TestUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{ BeforeAndAfterAll, FunSpec }

class SimpleKafkaIntTest extends FunSpec with BeforeAndAfterAll {
  val zkServer = new TestingServer()
  val config = kafkaConfig(zkServer.getConnectString)
  val kafkaServer = new KafkaServerStartable(config)

  override def beforeAll() = {
    kafkaServer.startup()
    info(s"started kafka on port [$kafkaPort]")
  }

  override def afterAll() = {
    info(s"stopping kafka on port [$kafkaPort]")
    kafkaServer.shutdown()
    zkServer.stop()
  }

  describe("producer") {
      val properties = {
        val props = new Properties()
        val compress = false
        val synchronously = true
        val brokerList = s"localhost:$kafkaPort"
        val batchSize = 1000
        val messageSendMaxRetries = 2
        val requestRequiredAcks =  1
        val clientId = "client_id_test"
        val codec = if (compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec

        props.put("compression.codec", codec.toString)
        props.put("producer.type", if (synchronously) "sync" else "async")
        props.put("metadata.broker.list", brokerList)
        props.put("batch.num.messages", batchSize.toString)
        props.put("message.send.max.retries", messageSendMaxRetries.toString)
        props.put("request.required.acks", requestRequiredAcks.toString)
        props.put("client.id", clientId.toString)
        props
      }

    it("can produce a message") {
      val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(properties))

      val topic = "testTopic"
      val message = "test message"
      val keyedMessage = new KeyedMessage[AnyRef, AnyRef](topic, message.getBytes("UTF8"))

      producer.send(keyedMessage)
    }
  }

  private def kafkaConfig(zkConnectString: String) = {
    val propsI: Iterator[Properties] = TestUtils.createBrokerConfigs(1).iterator
    assert(propsI.hasNext)
    val props = propsI.next()
    assert(props.containsKey("zookeeper.connect"))
    props.put("zookeeper.connect", zkConnectString)
    new KafkaConfig(props)
  }

  def kafkaBrokerString = s"localhost:${kafkaServer.serverConfig.port}"
  def zkConnectString = zkServer.getConnectString
  def kafkaPort = kafkaServer.serverConfig.port
}
