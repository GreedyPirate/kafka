package com.ttyc.api

import org.junit.Test

class ScalaTest {

  @Test
  def testGroupBy(): Unit = {

    val replicas = Seq(PartitionReplica("test-1", 0), PartitionReplica("test-1", 1),PartitionReplica("test-1", 2),
                       PartitionReplica("test-2", 0), PartitionReplica("test-2", 1),PartitionReplica("test-2", 2),
                       PartitionReplica("test-3", 0), PartitionReplica("test-3", 1),PartitionReplica("test-3", 2))
    val map: Map[Int, Seq[PartitionReplica]] = replicas.groupBy(_.replica)
    map.foreach{case (replica, obj) => {
      println(s"replicas: ${replicas}")
      println(s"obj: ${obj}")
    }}
  }


}

case class PartitionReplica(partition :String, replica :Int) {

}
