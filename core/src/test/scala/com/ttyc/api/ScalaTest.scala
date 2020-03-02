package com.ttyc.api

import org.junit.Test

class ScalaTest {

  @Test
  def testGroupBy(): Unit = {

    val replicas = Seq(PartitionReplica("test-1", 0), PartitionReplica("test-1", 1), PartitionReplica("test-1", 2),
      PartitionReplica("test-2", 0), PartitionReplica("test-2", 1), PartitionReplica("test-2", 2),
      PartitionReplica("test-3", 0), PartitionReplica("test-3", 1), PartitionReplica("test-3", 2))
    val map: Map[Int, Seq[PartitionReplica]] = replicas.groupBy(_.replica)
    map.foreach { case (replica, obj) => {
      println(s"replicas: ${replicas}")
      println(s"obj: ${obj}")
    }
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

}

case class PartitionReplica(partition: String, replica: Int) {

}
