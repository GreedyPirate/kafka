/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import kafka.cluster.Broker
import org.apache.kafka.common.TopicPartition

import scala.collection.{Seq, Set, mutable}

class ControllerContext {
  val stats = new ControllerStats

  var controllerChannelManager: ControllerChannelManager = null

  var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty
  // controller epoch 在kafka中epoch就相当于乐观锁的version
  var epoch: Int = KafkaController.InitialControllerEpoch - 1
  // 这是zk自带的version，通用的用于更新节点数据
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion - 1
  var allTopics: Set[String] = Set.empty
  // Map[Topic, Map[Partition, Seq[Replica]]] 存储每个topic的每个分区的副本集合
  private var partitionReplicaAssignmentUnderlying: mutable.Map[String, mutable.Map[Int, Seq[Int]]] = mutable.Map.empty

  // LeaderIsrAndControllerEpoch: {"controller_epoch":19,"leader":0,"version":1,"leader_epoch":57,"isr":[0,1,2]}
  val partitionLeadershipInfo: mutable.Map[TopicPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty

  val partitionsBeingReassigned: mutable.Map[TopicPartition, ReassignedPartitionsContext] = mutable.Map.empty
  // 这要等到向Controller发送LeaderAndIsr请求之后，才能初始化，key是brokerId
  val replicasOnOfflineDirs: mutable.Map[Int, Set[TopicPartition]] = mutable.Map.empty

  private var liveBrokersUnderlying: Set[Broker] = Set.empty
  private var liveBrokerIdsUnderlying: Set[Int] = Set.empty

  def partitionReplicaAssignment(topicPartition: TopicPartition): Seq[Int] = {
    partitionReplicaAssignmentUnderlying.getOrElse(topicPartition.topic, mutable.Map.empty)
      .getOrElse(topicPartition.partition, Seq.empty)
  }

  private def clearTopicsState(): Unit = {
    allTopics = Set.empty
    partitionReplicaAssignmentUnderlying.clear()
    partitionLeadershipInfo.clear()
    partitionsBeingReassigned.clear()
    replicasOnOfflineDirs.clear()
  }

  // 根据参数更新partitionReplicaAssignmentUnderlying缓存的topicPartition的副本信息
  def updatePartitionReplicaAssignment(topicPartition: TopicPartition, newReplicas: Seq[Int]): Unit = {
    // Map[Topic, Map[partition, Seq[replicas]]]
    partitionReplicaAssignmentUnderlying.getOrElseUpdate(topicPartition.topic, mutable.Map.empty)
      .put(topicPartition.partition, newReplicas)
  }

  def partitionReplicaAssignmentForTopic(topic : String): Map[TopicPartition, Seq[Int]] = {
    partitionReplicaAssignmentUnderlying.getOrElse(topic, Map.empty).map {
      case (partition, replicas) => (new TopicPartition(topic, partition), replicas)
    }.toMap
  }

  def allPartitions: Set[TopicPartition] = {
    partitionReplicaAssignmentUnderlying.flatMap {
      case (topic, topicReplicaAssignment) => topicReplicaAssignment.map {
        case (partition, _) => new TopicPartition(topic, partition)
      }
    }.toSet
  }

  // setter
  def liveBrokers_=(brokers: Set[Broker]) {
    liveBrokersUnderlying = brokers
    liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
  }

  // getter
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
  def liveBrokerIds = liveBrokerIdsUnderlying -- shuttingDownBrokerIds

  def liveOrShuttingDownBrokerIds = liveBrokerIdsUnderlying
  def liveOrShuttingDownBrokers = liveBrokersUnderlying

  def partitionsOnBroker(brokerId: Int): Set[TopicPartition] = {
    partitionReplicaAssignmentUnderlying.flatMap {
      case (topic, topicReplicaAssignment) => topicReplicaAssignment.filter {
        case (_, replicas) => replicas.contains(brokerId)
      }.map {
        case (partition, _) => new TopicPartition(topic, partition)
      }
    }.toSet
  }

  def isReplicaOnline(brokerId: Int, topicPartition: TopicPartition, includeShuttingDownBrokers: Boolean = false): Boolean = {
    // brokerId其实也是replica的id，这里liveBrokerIds包含brokerid，说明副本所在的broker还活着
    val brokerOnline = {
      if (includeShuttingDownBrokers) liveOrShuttingDownBrokerIds.contains(brokerId)
      else liveBrokerIds.contains(brokerId)
    }
    // broker 一定是在线的，replicasOnOfflineDirs正常情况下初始化就是空的
    brokerOnline && !replicasOnOfflineDirs.getOrElse(brokerId, Set.empty).contains(topicPartition)
  }

  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.flatMap { brokerId =>
      partitionReplicaAssignmentUnderlying.flatMap {
        case (topic, topicReplicaAssignment) => topicReplicaAssignment.collect {
          case (partition, replicas)  if replicas.contains(brokerId) =>
            PartitionAndReplica(new TopicPartition(topic, partition), brokerId)
        }
      }
    }
  }

  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionReplicaAssignmentUnderlying.getOrElse(topic, mutable.Map.empty).flatMap {
      case (partition, replicas) => replicas.map(r => PartitionAndReplica(new TopicPartition(topic, partition), r))
    }.toSet
  }

  def partitionsForTopic(topic: String): collection.Set[TopicPartition] = {
    partitionReplicaAssignmentUnderlying.getOrElse(topic, mutable.Map.empty).map {
      case (partition, _) => new TopicPartition(topic, partition)
    }.toSet
  }

  def allLiveReplicas(): Set[PartitionAndReplica] = {
    replicasOnBrokers(liveBrokerIds).filter { partitionAndReplica =>
      isReplicaOnline(partitionAndReplica.replica, partitionAndReplica.topicPartition)
    }
  }

  def replicasForPartition(partitions: collection.Set[TopicPartition]): collection.Set[PartitionAndReplica] = {
    partitions.flatMap { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(PartitionAndReplica(p, _))
    }
  }

  def resetContext(): Unit = {
    if (controllerChannelManager != null) {
      controllerChannelManager.shutdown()
      controllerChannelManager = null
    }
    shuttingDownBrokerIds.clear()
    epoch = 0
    epochZkVersion = 0
    clearTopicsState()
    liveBrokers = Set.empty
  }

  def removeTopic(topic: String): Unit = {
    allTopics -= topic
    partitionReplicaAssignmentUnderlying.remove(topic)
    partitionLeadershipInfo.foreach {
      case (topicPartition, _) if topicPartition.topic == topic => partitionLeadershipInfo.remove(topicPartition)
      case _ =>
    }
  }
}
