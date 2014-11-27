package simplekafkatest

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import java.util.Properties

import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable
import kafka.utils.TestUtils

import org.apache.curator.test.TestingServer

class SimpleKafkaIntTest extends FunSpec with BeforeAndAfterAll {
  val zkServer = new TestingServer()
  val config = getKafkaConfig(zkServer.getConnectString)
  val kafkaServer = new KafkaServerStartable(config)

  override def beforeAll() = kafkaServer.startup()

  override def afterAll() = {
    kafkaServer.shutdown()
    zkServer.stop()
  }

  describe("Kafka Server") {
    it("should start") (pending)
  }

  private def getKafkaConfig(zkConnectString: String) = {
    val propsI: Iterator[Properties] = TestUtils.createBrokerConfigs(1).iterator
    assert(propsI.hasNext)
    val props = propsI.next()
    assert(props.containsKey("zookeeper.connect"))
    props.put("zookeeper.connect", zkConnectString)
    new KafkaConfig(props)
  }

  def getKafkaBrokerString = s"localhost:${kafkaServer.serverConfig.port}"
  def getZkConnectString = zkServer.getConnectString
  def getKafkaPort = kafkaServer.serverConfig.port
}
