package com.ttyc.api

class OptionTest {

  @Test
  def testFlatMap(): Unit = {

    getLeader match {
      case Some(leader) =>
        println(leader)
    }



    /*leaderReplicaIdOpt match {
      case Some(leader) =>

    }*/
  }

  def getLeader() :Option[Int] = {
    var leaderReplicaIdOpt: Option[Int] = Some(1)

    leaderReplicaIdOpt.filter(_ == 1).flatMap(getReplica)
  }

  def getReplica(id :Int) :Option[Int] = {
    Some(20)
  }
}