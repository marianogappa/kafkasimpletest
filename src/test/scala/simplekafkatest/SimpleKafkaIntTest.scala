package simplekafkatest

import java.util.{ UUID, Properties }

import kafka.common._
import kafka.consumer._
import kafka.message._
import kafka.producer._
import kafka.serializer._
import kafka.utils.TestUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{ BeforeAndAfterAll, FunSpec, Matchers }

class SimpleKafkaIntTest extends KafkaIntTestBase {

  describe("producer and consumer") {
    it("can produce and consume a message") {
      val topic = UUID.randomUUID.toString
      val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(producerProperties))
      val message = "test message"
      val keyedMessage = new KeyedMessage[AnyRef, AnyRef](topic, message.getBytes("UTF-8"))
      producer.send(keyedMessage)

      val consumer = Consumer.create(new ConsumerConfig(consumerProperties()))
      val filterSpec = new Whitelist(topic)
      val stream = consumer.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).head

      stream.take(1).map(messageAndTopic ⇒ new String(messageAndTopic.message(), "UTF-8")).head shouldBe message
    }
  }

  describe("consumer") {
    it("times out") {
      val topic = UUID.randomUUID.toString
      val consumer = Consumer.create(new ConsumerConfig(consumerProperties(timeoutMillis = 300)))
      val filterSpec = new Whitelist(topic)
      val stream = consumer.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).head

      intercept[ConsumerTimeoutException] {
        stream.take(1).map(messageAndTopic ⇒ new String(messageAndTopic.message(), "UTF-8")).head
      }
    }
  }

}

