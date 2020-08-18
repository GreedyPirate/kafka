package com.ttyc.api

import java.util.Properties

import kafka.cluster.BrokerEndPoint
import kafka.server.BrokerIdAndFetcherId
import kafka.utils.ToolsUtils
import kafka.zk.IsrChangeNotificationSequenceZNode.SequenceNumberPrefix
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.junit.Test

import scala.collection.mutable

class ScalaTest {

  @Test
  def testGroupBy(): Unit = {

    val replicas = Seq(
      PartitionReplica("test-1", 0), PartitionReplica("test-1", 1), PartitionReplica("test-1", 2),
      PartitionReplica("test-2", 0), PartitionReplica("test-2", 1), PartitionReplica("test-2", 2),
      PartitionReplica("test-3", 0), PartitionReplica("test-3", 1), PartitionReplica("test-3", 2)
    )
    replicas.groupBy(_.replica).map {  case (replicaId, reps) =>
    }
  }

  @Test
  def testFindOrElse: Unit = {
    val isr = Seq(1)
    val liveReplicas = Seq(2, 3)
    val assignment = Seq(0, 1, 2, 3)
    val leader = assignment.find(id => liveReplicas.contains(id) && isr.contains(id)).orElse {
      assignment.find(liveReplicas.contains)
    }
    println(s"leader is $leader")
  }

  @Test
  def testAnd: Unit = {
    val isr = Seq(0, 1, 2).toSet
    val newReplica = Seq(1, 2,3).toSet
    val caughtUpReplicas = isr & newReplica
    println(s"ret is $caughtUpReplicas")

    val isEqual = caughtUpReplicas == newReplica
    println(s"contains ? $isEqual")

    println(s"${caughtUpReplicas.size}/${newReplica.size} replicas have caught up with the leader for " +
      s"partition 0 being reassigned. Replica(s) " +
      s"${(newReplica -- isr.toSet).mkString(",")} still need to catch up")

    val all = isr ++ newReplica
    println(s"all is $all")
  }

  @Test
  def TestOr: Unit = {
    val isr = Seq(1,2,3).toSet
    var replica = Seq(3,4).toSet

    val all = isr | replica
    println(s"all is $all")
  }

  @Test
  def testExist: Unit = {
    val replicas = Seq(0,1,2,3)
    val ret = replicas.exists(r => !(r > 4))
    println(s"ret is $ret")

    print(replicas.exists(r => r > 4))
  }

  @Test
  def testNot: Unit = {
    val isr = Seq(1,2,3,4).toSet
    var replica = Seq(5,7,4).toSet

    val newAdd = replica -- isr
    println(s"newAdd is $newAdd")

  }

  @Test
  def testOverride: Unit = {
/*    val originals = new Properties
    originals.put("time", Int(1000))
    originals.put("ack",Int(1))
    originals.put("size",Int(100))

    val overrides = new Properties
    overrides.put("size", Int(200))*/

//    originals ++= overrides
  }

  @Test
  def testFetcherId: Unit = {
    val topic = "test";
    val partitionId = 1;
    val id = Utils.abs(31 * topic.hashCode() + partitionId) % 5
    println(s"id = $id")
  }

  @Test
  def testGroupbyBrokerAndFetcher ={
    val fetchMap = Map(
      new TopicPartition("test",0) -> BrokerIdAndInitialOffset(0, 100),
      new TopicPartition("test",0) -> BrokerIdAndInitialOffset(1, 300),
      new TopicPartition("test",2) -> BrokerIdAndInitialOffset(1, 400),
      new TopicPartition("test",3) -> BrokerIdAndInitialOffset(1, 500),
      new TopicPartition("test",4) -> BrokerIdAndInitialOffset(0, 600),
      new TopicPartition("test",5) -> BrokerIdAndInitialOffset(2, 700),
      new TopicPartition("test",6) -> BrokerIdAndInitialOffset(0, 800)
    )
    var partitionsPerBroker = fetchMap.groupBy({case (topicPartition, brokerIdAndInitialOffset) =>
      BrokerIdAndFetcherId(brokerIdAndInitialOffset.brokerId, getFetchId(topicPartition.topic(), topicPartition.partition()))
    })

    for (elem <- partitionsPerBroker) {
      val brokerFetchId = elem._1
      println(s"$brokerFetchId ----->")
      elem._2.foreach(entry => {
        val tp = entry._1
        val brokerOffset = entry._2
        println(s"$tp ===== $brokerOffset")
      })

      println("===========================")
    }
  }

  def getFetchId(topic: String, partitionId: Int) :Int ={
    Utils.abs(31 * topic.hashCode() + partitionId) % 5
  }

  @Test
  def testLeaderEpochs: Unit = {
    val leadersEpoch = Seq(1,2,3,4,5);
    val followerEpoch = 3;

    val (subsequent, previous) = leadersEpoch.partition(e => e > followerEpoch)
    println(s"subsequent is empty? ${subsequent.isEmpty}, subsequent is $subsequent, previous is empty ${previous.isEmpty}, previous is $previous")

  }

  @Test
  def testDate: Unit={
    var date = ToolsUtils.formateDate(System.currentTimeMillis())
    printf(s"date = $date")
  }

  @Test
  def testIsrSeq: Unit = {
    val number = sequenceNumber("/isr_change_notification/isr_change_0000000001")
    println(s"num is : $number")
  }
  def sequenceNumber(path: String) = path.substring(path.lastIndexOf(SequenceNumberPrefix) + SequenceNumberPrefix.length)

  @Test
  def testStrMap: Unit = {
    val str = "k1:v1,k2:v2,k3:v3"
    val tuples = str.split(",").map(t => {
      val kv = t.split(":")
      (kv(0).trim, kv(1).trim)
    })
    val map = tuples.toMap
    println(s"${map.getClass.getName}\n$map")
  }
}

case class BrokerIdAndInitialOffset(brokerId: Int, initOffset: Long)

case class PartitionReplica(partition: String, replica: Int) {

}
