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
package kafka.cluster


import java.util.concurrent.locks.ReentrantReadWriteLock

import com.yammer.metrics.core.Gauge
import kafka.api.{LeaderAndIsr, Request}
import kafka.common.UnexpectedAppendOffsetException
import kafka.controller.KafkaController
import kafka.log.{LogAppendInfo, LogConfig}
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.zk.AdminZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ReplicaNotAvailableException, NotEnoughReplicasException, NotLeaderForPartitionException, PolicyViolationException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.{EpochEndOffset, LeaderAndIsrRequest}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.Map

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
class Partition(val topic: String,
                val partitionId: Int,
                time: Time,
                replicaManager: ReplicaManager,
                val isOffline: Boolean = false) extends Logging with KafkaMetricsGroup {

  val topicPartition = new TopicPartition(topic, partitionId)

  // Do not use replicaManager if this partition is ReplicaManager.OfflinePartition
  private val localBrokerId = if (!isOffline) replicaManager.config.brokerId else -1
  private val logManager = if (!isOffline) replicaManager.logManager else null
  private val zkClient = if (!isOffline) replicaManager.zkClient else null
  // allReplicasMap includes both assigned replicas and the future replica if there is ongoing replica movement
  // 每一个副本的id，对应的Replica
  private val allReplicasMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  // start offset for 'leaderEpoch' above (leader epoch of the current leader for this partition),
  // defined when this broker is leader for partition
  @volatile private var leaderEpochStartOffsetOpt: Option[Long] = None
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = s"[Partition $topicPartition broker=$localBrokerId] "

  private def isReplicaLocal(replicaId: Int) : Boolean = replicaId == localBrokerId || replicaId == Request.FutureLocalReplicaId

  private val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  // Do not create metrics if this partition is ReplicaManager.OfflinePartition
  if (!isOffline) {
    newGauge("UnderReplicated",
      new Gauge[Int] {
        def value = {
          if (isUnderReplicated) 1 else 0
        }
      },
      tags
    )

    newGauge("InSyncReplicasCount",
      new Gauge[Int] {
        def value = {
          if (isLeaderReplicaLocal) inSyncReplicas.size else 0
        }
      },
      tags
    )

    newGauge("UnderMinIsr",
      new Gauge[Int] {
        def value = {
          if (isUnderMinIsr) 1 else 0
        }
      },
      tags
    )

    newGauge("ReplicasCount",
      new Gauge[Int] {
        def value = {
          if (isLeaderReplicaLocal) assignedReplicas.size else 0
        }
      },
      tags
    )

    newGauge("LastStableOffsetLag",
      new Gauge[Long] {
        def value = {
          leaderReplicaIfLocal.map { replica =>
            replica.highWatermark.messageOffset - replica.lastStableOffset.messageOffset
          }.getOrElse(0)
        }
      },
      tags
    )
  }

  private def isLeaderReplicaLocal: Boolean = leaderReplicaIfLocal.isDefined

  def isUnderReplicated: Boolean =
    isLeaderReplicaLocal && inSyncReplicas.size < assignedReplicas.size

  def isUnderMinIsr: Boolean = {
    leaderReplicaIfLocal match {
      case Some(leaderReplica) =>
        inSyncReplicas.size < leaderReplica.log.get.config.minInSyncReplicas
      case None =>
        false
    }
  }

  /**
    * Create the future replica if 1) the current replica is not in the given log directory and 2) the future replica
    * does not exist. This method assumes that the current replica has already been created.
    *
    * @param logDir log directory
    * @return true iff the future replica is created
    */
  def maybeCreateFutureReplica(logDir: String): Boolean = {
    // The writeLock is needed to make sure that while the caller checks the log directory of the
    // current replica and the existence of the future replica, no other thread can update the log directory of the
    // current replica or remove the future replica.
    inWriteLock(leaderIsrUpdateLock) {
      val currentReplica = getReplica().get
      if (currentReplica.log.get.dir.getParent == logDir)
        false
      else if (getReplica(Request.FutureLocalReplicaId).isDefined) {
        val futureReplicaLogDir = getReplica(Request.FutureLocalReplicaId).get.log.get.dir.getParent
        if (futureReplicaLogDir != logDir)
          throw new IllegalStateException(s"The future log dir $futureReplicaLogDir of $topicPartition is different from the requested log dir $logDir")
        false
      } else {
        getOrCreateReplica(Request.FutureLocalReplicaId)
        true
      }
    }
  }

  /**
    * @param replicaId
    * @param isNew 就是LeaderAndIsr中的isNew
    */
  def getOrCreateReplica(replicaId: Int = localBrokerId, isNew: Boolean = false): Replica = {
    // 根据replicaId从allReplicasMap获取Replica
    // 没有根据传入的函数创建Replica
    allReplicasMap.getAndMaybePut(replicaId, {
      if (isReplicaLocal(replicaId)) { // replicaId是否是本地brokerId
        val adminZkClient = new AdminZkClient(zkClient)
        // 获取 /config/topic/topic名称 节点的数据，即topic配置
        val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
        val config = LogConfig.fromProps(logManager.currentDefaultConfig.originals, props)
        val log = logManager.getOrCreateLog(topicPartition, config, isNew, replicaId == Request.FutureLocalReplicaId)
        val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParent)
        // 读取读取replication-offset-checkpoint文件，应该记录的是每个分区的hw
        val offsetMap = checkpoint.read()
        if (!offsetMap.contains(topicPartition))
          info(s"No checkpointed highwatermark is found for partition $topicPartition")
        // checkpoint中的HW和LEO的最小值作为replica初始HW
        val offset = math.min(offsetMap.getOrElse(topicPartition, 0L), log.logEndOffset)
        new Replica(replicaId, topicPartition, time, offset, Some(log))
      } else {
        new Replica(replicaId, topicPartition, time)
      }
    })
  }

  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = Option(allReplicasMap.get(replicaId))

  def getReplicaOrException(replicaId: Int = localBrokerId): Replica =
    getReplica(replicaId).getOrElse(
      throw new ReplicaNotAvailableException(s"Replica $replicaId is not available for partition $topicPartition"))

  def leaderReplicaIfLocal: Option[Replica] =
    leaderReplicaIdOpt.filter(_ == localBrokerId).flatMap(getReplica)

  def addReplicaIfNotExists(replica: Replica): Replica =
    allReplicasMap.putIfNotExists(replica.brokerId, replica)

  def assignedReplicas: Set[Replica] =
    allReplicasMap.values.filter(replica => Request.isValidBrokerId(replica.brokerId)).toSet

  def allReplicas: Set[Replica] =
    allReplicasMap.values.toSet

  private def removeReplica(replicaId: Int) {
    allReplicasMap.remove(replicaId)
  }

  def futureReplicaDirChanged(newDestinationDir: String): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      getReplica(Request.FutureLocalReplicaId) match {
        case Some(futureReplica) =>
          if (futureReplica.log.get.dir.getParent != newDestinationDir)
            true
          else
            false
        case None => false
      }
    }
  }

  def removeFutureLocalReplica(deleteFromLogDir: Boolean = true) {
    inWriteLock(leaderIsrUpdateLock) {
      allReplicasMap.remove(Request.FutureLocalReplicaId)
      if (deleteFromLogDir)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  // Return true iff the future replica exists and it has caught up with the current replica for this partition
  // Only ReplicaAlterDirThread will call this method and ReplicaAlterDirThread should remove the partition
  // from its partitionStates if this method returns true
  def maybeReplaceCurrentWithFutureReplica(): Boolean = {
    val replica = getReplica().get
    val futureReplicaLEO = getReplica(Request.FutureLocalReplicaId).map(_.logEndOffset)
    if (futureReplicaLEO.contains(replica.logEndOffset)) {
      // The write lock is needed to make sure that while ReplicaAlterDirThread checks the LEO of the
      // current replica, no other thread can update LEO of the current replica via log truncation or log append operation.
      inWriteLock(leaderIsrUpdateLock) {
        getReplica(Request.FutureLocalReplicaId) match {
          case Some(futureReplica) =>
            if (replica.logEndOffset == futureReplica.logEndOffset) {
              logManager.replaceCurrentWithFutureLog(topicPartition)
              replica.log = futureReplica.log
              futureReplica.log = None
              allReplicasMap.remove(Request.FutureLocalReplicaId)
              true
            } else false
          case None =>
            // Future replica is removed by a non-ReplicaAlterLogDirsThread before this method is called
            // In this case the partition should have been removed from state of the ReplicaAlterLogDirsThread
            // Return false so that ReplicaAlterLogDirsThread does not have to remove this partition from the state again to avoid race condition
            false
        }
      }
    } else false
  }

  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      allReplicasMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      leaderEpochStartOffsetOpt = None
      removePartitionMetrics()
      logManager.asyncDelete(topicPartition)
      logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  def getLeaderEpoch: Int = this.leaderEpoch

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  def makeLeader(controllerId: Int, partitionStateInfo: LeaderAndIsrRequest.PartitionState, correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      // 请求中的AR
      val newAssignedReplicas = partitionStateInfo.basePartitionState.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      // Partition里也有一份controllerEpoch
      controllerEpoch = partitionStateInfo.basePartitionState.controllerEpoch
      // add replicas that are new
      // 获取isr对应的Replica
      val newInSyncReplicas = partitionStateInfo.basePartitionState.isr.asScala.map(r => getOrCreateReplica(r, partitionStateInfo.isNew)).toSet
      // remove assigned replicas that have been removed by the controller
      // 该分区已有的副本-新分配的副本=controller要移除的副本，从本地缓存allReplicasMap = new Pool[Int, Replica]中删除
      (assignedReplicas.map(_.brokerId) -- newAssignedReplicas).foreach(removeReplica)
      // 新的isr是controller传过来的,更新
      inSyncReplicas = newInSyncReplicas
      // 获取replicas对应的Replica
      newAssignedReplicas.foreach(id => getOrCreateReplica(id, partitionStateInfo.isNew))

      // 不是说当前replica是leader副本，而是说它即将要成为leader
      val leaderReplica = getReplica().get
      // 获取leader副本的LEO
      val leaderEpochStartOffset = leaderReplica.logEndOffset.messageOffset
      info(s"$topicPartition starts at Leader Epoch ${partitionStateInfo.basePartitionState.leaderEpoch} from " +
        s"offset $leaderEpochStartOffset. Previous Leader Epoch was: $leaderEpoch")

      //We cache the leader epoch here, persisting it only if it's local (hence having a log dir)
      // 更新leaderEpoch，以及这一届leaderEpoch对应的StartOffset
      leaderEpoch = partitionStateInfo.basePartitionState.leaderEpoch
      leaderEpochStartOffsetOpt = Some(leaderEpochStartOffset)
      zkVersion = partitionStateInfo.basePartitionState.zkVersion

      // In the case of successive leader elections in a short time period, a follower may have
      // entries in its log from a later epoch than any entry in the new leader's log. In order
      // to ensure that these followers can truncate to the right offset, we must cache the new
      // leader epoch and the start offset since it should be larger than any epoch that a follower
      // would try to query.
      // 在短暂的时间内连续选举的情况下，一个follower的epoch可能比leader的epoch大，为了保证follower能正确截断日志，
      // 我们必须保存新的leader epoch和它的start offset(leader 副本当前的LEO)

      // 将leader epoch及其开始位移写入文件
      leaderReplica.epochs.foreach { epochCache =>
        epochCache.assign(leaderEpoch, leaderEpochStartOffset)
      }

      // 如果分区的leader副本就是当前broker，就不用变更了
      // 注：看becomeLeaderOrFollower方法的星号注释
      val isNewLeader = !leaderReplicaIdOpt.contains(localBrokerId)

      val curLeaderLogEndOffset = leaderReplica.logEndOffset.messageOffset // leader副本的LEO
      val curTimeMs = time.milliseconds
      // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
      // 更新副本的同步时间，LEO
      (assignedReplicas - leaderReplica).foreach { replica =>
        val lastCaughtUpTimeMs = if (inSyncReplicas.contains(replica)) curTimeMs else 0L
        replica.resetLastCaughtUpTime(curLeaderLogEndOffset, curTimeMs, lastCaughtUpTimeMs)
      }

      if (isNewLeader) {
        // construct the high watermark metadata for the new leader replica
        // 初始化HW(大概率就是当前的HW)
        leaderReplica.convertHWToLocalOffsetMetadata()
        // mark local replica as the leader after converting hw
        leaderReplicaIdOpt = Some(localBrokerId)
        // reset log end offset for remote replicas
        // 初始化同步相关的一堆参数
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      // we may need to increment high watermark since ISR could be down to 1
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      // HW增加了，fetch请求的max.byte，produce请求的ack=-1等待副本同步就可以try complete了，
      tryCompleteDelayedRequests()
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change and the new epoch is equal or one
   *  greater (that is, no updates have been missed), return false to indicate to the
   *  replica manager that state is already correct and the become-follower steps can be skipped
   * @param controllerId
   * @param partitionStateInfo
   * @param correlationId
   * @return leader是否更新了
   */
  def makeFollower(controllerId: Int, partitionStateInfo: LeaderAndIsrRequest.PartitionState, correlationId: Int): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val newAssignedReplicas = partitionStateInfo.basePartitionState.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId = partitionStateInfo.basePartitionState.leader
      val oldLeaderEpoch = leaderEpoch
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.basePartitionState.controllerEpoch
      // add replicas that are new
      newAssignedReplicas.foreach(r => getOrCreateReplica(r, partitionStateInfo.isNew))
      // remove assigned replicas that have been removed by the controller
      // 删除缓存里不要的副本了
      (assignedReplicas.map(_.brokerId) -- newAssignedReplicas).foreach(removeReplica)

      inSyncReplicas = Set.empty[Replica] // ISR为空 emm...
      leaderEpoch = partitionStateInfo.basePartitionState.leaderEpoch
      leaderEpochStartOffsetOpt = None
      zkVersion = partitionStateInfo.basePartitionState.zkVersion

      // If the leader is unchanged and the epochs are no more than one change apart, indicate that no follower changes are required
      // Otherwise, we missed a leader epoch update, which means the leader's log may have been truncated prior to the current epoch.
      // leader是否更新了
      if (leaderReplicaIdOpt.contains(newLeaderBrokerId) && (leaderEpoch == oldLeaderEpoch || leaderEpoch == oldLeaderEpoch + 1)) {
        false
      }
      else {
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the follower's state in the leader based on the last fetch request. See
   * [[kafka.cluster.Replica#updateLogReadResult]] for details.
   *
   * @return true if the leader's log start offset or high watermark have been updated
   */
  def updateReplicaLogReadResult(replica: Replica, logReadResult: LogReadResult): Boolean = {
    val replicaId = replica.brokerId
    // No need to calculate low watermark if there is no delayed DeleteRecordsRequest
    val oldLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L
    replica.updateLogReadResult(logReadResult)
    val newLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L
    // check if the LW of the partition has incremented
    // since the replica's logStartOffset may have incremented
    val leaderLWIncremented = newLeaderLW > oldLeaderLW
    // check if we need to expand ISR to include this replica
    // if it is not in the ISR yet
    val leaderHWIncremented = maybeExpandIsr(replicaId, logReadResult)

    val result = leaderLWIncremented || leaderHWIncremented
    // some delayed operations may be unblocked after HW or LW changed
    if (result)
      tryCompleteDelayedRequests()

    debug(s"Recorded replica $replicaId log end offset (LEO) position ${logReadResult.info.fetchOffsetMetadata.messageOffset}.")
    result
  }

  /**
   * Check and maybe expand the ISR of the partition.
   * A replica will be added to ISR if its LEO >= current hw of the partition and it is caught up to
   * an offset within the current leader epoch. A replica must be caught up to the current leader
   * epoch before it can join ISR, because otherwise, if there is committed data between current
   * leader's HW and LEO, the replica may become the leader before it fetches the committed data
   * and the data will be lost.
   *
   * Technically, a replica shouldn't be in ISR if it hasn't caught up for longer than replicaLagTimeMaxMs,
   * even if its log end offset is >= HW. However, to be consistent with how the follower determines
   * whether a replica is in-sync, we only check HW.
   *
   * This function can be triggered when a replica's LEO has incremented.
   *
   * @return true if the high watermark has been updated
   */
  def maybeExpandIsr(replicaId: Int, logReadResult: LogReadResult): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val replica = getReplica(replicaId).get
          val leaderHW = leaderReplica.highWatermark
          val fetchOffset = logReadResult.info.fetchOffsetMetadata.messageOffset
          if (!inSyncReplicas.contains(replica) &&
             assignedReplicas.map(_.brokerId).contains(replicaId) &&
             replica.logEndOffset.offsetDiff(leaderHW) >= 0 &&
             leaderEpochStartOffsetOpt.exists(fetchOffset >= _)) {
            val newInSyncReplicas = inSyncReplicas + replica
            info(s"Expanding ISR from ${inSyncReplicas.map(_.brokerId).mkString(",")} " +
              s"to ${newInSyncReplicas.map(_.brokerId).mkString(",")}")
            // update ISR in ZK and cache
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }
          // check if the HW of the partition can now be incremented
          // since the replica may already be in the ISR and its LEO has just incremented
          maybeIncrementLeaderHW(leaderReplica, logReadResult.fetchTimeMs)
        case None => false // nothing to do if no longer leader
      }
    }
  }

  /*
   * Returns a tuple where the first element is a boolean indicating whether enough replicas reached `requiredOffset`
   * and the second element is an error (which would be `Errors.NONE` for no error).
   *
   * Note that this method will only be called if requiredAcks = -1 and we are waiting for all replicas in ISR to be
   * fully caught up to the (local) leader's offset corresponding to this produce request before we acknowledge the
   * produce request.
   */
  /**
    * 这里应该就是在检查是否所有ISR中副本，已经同步完leader了
    * @param requiredOffset lastOffset + 1. 就是LEO
    * @return
    */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
    leaderReplicaIfLocal match {
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference
        // ISR Set[Replica]
        val curInSyncReplicas = inSyncReplicas

        if (isTraceEnabled) {
          def logEndOffsetString(r: Replica) = s"broker ${r.brokerId}: ${r.logEndOffset.messageOffset}"
          val (ackedReplicas, awaitingReplicas) = curInSyncReplicas.partition { replica =>
            replica.logEndOffset.messageOffset >= requiredOffset
          }
          trace(s"Progress awaiting ISR acks for offset $requiredOffset: acked: ${ackedReplicas.map(logEndOffsetString)}, " +
            s"awaiting ${awaitingReplicas.map(logEndOffsetString)}")
        }

        val minIsr = leaderReplica.log.get.config.minInSyncReplicas
        if (leaderReplica.highWatermark.messageOffset >= requiredOffset) {
          /*
           * The topic may be configured not to accept messages if there are not enough replicas in ISR
           * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
           */
          // ISR必须大于等于min.insync.replicas
          if (minIsr <= curInSyncReplicas.size)
            (true, Errors.NONE)
          else
            // 否则报错
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
        } else
          (false, Errors.NONE)
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
   * HW取决于ISR副本或即将追上的副本中最小的LEO，如果一个副本即将追上，但是它的LEO小于HW，我们会在更新HW之前等它跟上HW，
   * 当在ISR中只有一个leader副本时，如果我们不这样做，follower副本会一直落后HW，而永远添加不进ISR
   * The HW is determined by the smallest log end offset among all replicas that are in sync or are considered caught-up.
   * This way, if a replica is considered caught-up, but its log end offset is smaller than HW, we will wait for this
   * replica to catch up to the HW before advancing the HW. This helps the situation when the ISR only includes the
   * leader replica and a follower tries to catch up. If we don't wait for the follower when advancing the HW, the
   * follower's log end offset may keep falling behind the HW (determined by the leader's log end offset) and therefore
   * will never be added to ISR.
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  private def maybeIncrementLeaderHW(leaderReplica: Replica, curTime: Long = time.milliseconds): Boolean = {
    // ISR或considered caught-up的副本
    val allLogEndOffsets = assignedReplicas.filter { replica =>
      // 落后时间<=replica.lag.time.max.ms
      curTime - replica.lastCaughtUpTimeMs <= replicaManager.config.replicaLagTimeMaxMs || inSyncReplicas.contains(replica)
    }.map(_.logEndOffset)
    // 所有副本最小值
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering) // 传的是一个比较器
    val oldHighWatermark = leaderReplica.highWatermark

    // 当前副本的hw比 min LEO小，或者(二者相等，但是后者已经在新的segment上了)
    // Ensure that the high watermark increases monotonically. We also update the high watermark when the new
    // offset metadata is on a newer segment, which occurs whenever the log is rolled to a new segment.
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset ||  // 比较两者的segment baseOffset
      (oldHighWatermark.messageOffset == newHighWatermark.messageOffset && oldHighWatermark.onOlderSegment(newHighWatermark))) {
      leaderReplica.highWatermark = newHighWatermark
      debug(s"High watermark updated to $newHighWatermark")
      true
    } else {
      // 所以说hw的更新不取决于传进来的oldHighWatermark，而是当面计算一次min LEO，如果它比oldHighWatermark大才更新
      def logEndOffsetString(r: Replica) = s"replica ${r.brokerId}: ${r.logEndOffset}"
      debug(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old hw $oldHighWatermark. " +
        s"All current LEOs are ${assignedReplicas.map(logEndOffsetString)}")
      false
    }
  }

  /**
   * The low watermark offset value, calculated only if the local replica is the partition leader
   * It is only used by leader broker to decide when DeleteRecordsRequest is satisfied. Its value is minimum logStartOffset of all live replicas
   * Low watermark will increase when the leader broker receives either FetchRequest or DeleteRecordsRequest.
   */
  def lowWatermarkIfLeader: Long = {
    if (!isLeaderReplicaLocal)
      throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
    val logStartOffsets = allReplicas.collect {
      case replica if replicaManager.metadataCache.isBrokerAlive(replica.brokerId) || replica.brokerId == Request.FutureLocalReplicaId => replica.logStartOffset
    }
    CoreUtils.min(logStartOffsets, 0L)
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(topicPartition)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
    replicaManager.tryCompleteDelayedDeleteRecords(requestKey)
  }

  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.nonEmpty) {
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.nonEmpty)
            info("Shrinking ISR from %s to %s".format(inSyncReplicas.map(_.brokerId).mkString(","),
              newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1

            replicaManager.isrShrinkRate.mark()
            maybeIncrementLeaderHW(leaderReplica)
          } else {
            false
          }

        case None => false // do nothing if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
     * there are two cases that will be handled here -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
     *                     the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
     *                    then the follower is lagging and should be removed from the ISR
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
     * the last time when the replica was fully caught up. If either of the above conditions
     * is violated, that replica is considered to be out of sync
     *
     **/
    val candidateReplicas = inSyncReplicas - leaderReplica

    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if (laggingReplicas.nonEmpty)
      debug("Lagging replicas are %s".format(laggingReplicas.map(_.brokerId).mkString(",")))

    laggingReplicas
  }

  private def doAppendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Unit = {
    inReadLock(leaderIsrUpdateLock) {
      if (isFuture) {
        // The read lock is needed to handle race condition if request handler thread tries to
        // remove future replica after receiving AlterReplicaLogDirsRequest.
        inReadLock(leaderIsrUpdateLock) {
          getReplica(Request.FutureLocalReplicaId) match {
            case Some(replica) => replica.log.get.appendAsFollower(records)
            case None => // Future replica is removed by a non-ReplicaAlterLogDirsThread before this method is called
          }
        }
      } else {
        // The read lock is needed to prevent the follower replica from being updated while ReplicaAlterDirThread
        // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
        getReplicaOrException().log.get.appendAsFollower(records)
      }
    }
  }

  def appendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean) {
    try {
      doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
    } catch {
      case e: UnexpectedAppendOffsetException =>
        val replica = if (isFuture) getReplicaOrException(Request.FutureLocalReplicaId) else getReplicaOrException()
        val logEndOffset = replica.logEndOffset.messageOffset
        if (logEndOffset == replica.logStartOffset &&
            e.firstOffset < logEndOffset && e.lastOffset >= logEndOffset) {
          // This may happen if the log start offset on the leader (or current replica) falls in
          // the middle of the batch due to delete records request and the follower tries to
          // fetch its first offset from the leader.
          // We handle this case here instead of Log#append() because we will need to remove the
          // segment that start with log start offset and create a new one with earlier offset
          // (base offset of the batch), which will move recoveryPoint backwards, so we will need
          // to checkpoint the new recovery point before we append
          val replicaName = if (isFuture) "future replica" else "follower"
          info(s"Unexpected offset in append to $topicPartition. First offset ${e.firstOffset} is less than log start offset ${replica.logStartOffset}." +
               s" Since this is the first record to be appended to the $replicaName's log, will start the log from offset ${e.firstOffset}.")
          truncateFullyAndStartAt(e.firstOffset, isFuture)
          doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
        } else
          throw e
    }
  }

  /**
    * 熟悉的三个参数
    * @param records
    * @param isFromClient
    * @param requiredAcks
    * @return
    */
  def   appendRecordsToLeader(records: MemoryRecords, isFromClient: Boolean, requiredAcks: Int = 0): LogAppendInfo = {
    // inReadLock是一个柯里化函数，第二个参数是一个函数，返回值是LogAppendInfo和HW是否增加的bool值
    // 相当于给方法加了读锁
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      // leaderReplicaIfLocal是在调用方法，返回Option[Replica]对象
      leaderReplicaIfLocal match {
        //如果存在的话
        case Some(leaderReplica) =>
          // 获取Replica中的Log对象
          val log = leaderReplica.log.get
          // min.insync.replicas参数
          val minIsr = log.config.minInSyncReplicas
          // Set[Replica] ISR大小
          val inSyncSize = inSyncReplicas.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          // 如果isr的个数没有满足min.insync.replicas就报错，需要知道的是min.insync.replicas是和ack=-1一起使用的
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException("Number of insync replicas for partition %s is [%d], below required minimum [%d]"
              .format(topicPartition, inSyncSize, minIsr))
          }

          // 真正的消息追加交给Log对象
          val info = log.appendAsLeader(records, leaderEpoch = this.leaderEpoch, isFromClient)
          // 写入完消息，尝试触发Fetch请求，比如满足消费者的fetch.max.bytes
          // probably unblock some follower fetch requests since log end offset has been updated
          replicaManager.tryCompleteDelayedFetch(TopicPartitionOperationKey(this.topic, this.partitionId))
          // we may need to increment high watermark since ISR could be down to 1
          // 新增leader的HW
          (info, maybeIncrementLeaderHW(leaderReplica))

        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    info
  }

  def logStartOffset: Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal.map(_.log.get.logStartOffset).getOrElse(-1)
    }
  }

  /**
   * Update logStartOffset and low watermark if 1) offset <= highWatermark and 2) it is the leader replica.
   * This function can trigger log segment deletion and log rolling.
   *
   * Return low watermark of the partition.
   */
  def deleteRecordsOnLeader(offset: Long): Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          if (!leaderReplica.log.get.config.delete)
            throw new PolicyViolationException("Records of partition %s can not be deleted due to the configured policy".format(topicPartition))
          leaderReplica.maybeIncrementLogStartOffset(offset)
          lowWatermarkIfLeader
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }
  }

  /**
    * Truncate the local log of this partition to the specified offset and checkpoint the recovery point to this offset
    *
    * @param offset offset to be used for truncation
    * @param isFuture True iff the truncation should be performed on the future log of this partition
    */
  def truncateTo(offset: Long, isFuture: Boolean) {
    // The read lock is needed to prevent the follower replica from being truncated while ReplicaAlterDirThread
    // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateTo(Map(topicPartition -> offset), isFuture = isFuture)
    }
  }

  /**
    * Delete all data in the local log of this partition and start the log at the new offset
    *
    * @param newOffset The new offset to start the log with
    * @param isFuture True iff the truncation should be performed on the future log of this partition
    */
  def truncateFullyAndStartAt(newOffset: Long, isFuture: Boolean) {
    // The read lock is needed to prevent the follower replica from being truncated while ReplicaAlterDirThread
    // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateFullyAndStartAt(topicPartition, newOffset, isFuture = isFuture)
    }
  }

  /**
    * @param leaderEpoch Requested leader epoch
    * @return The requested leader epoch and the end offset of this leader epoch, or if the requested
    *         leader epoch is unknown, the leader epoch less than the requested leader epoch and the end offset
    *         of this leader epoch. The end offset of a leader epoch is defined as the start
    *         offset of the first leader epoch larger than the leader epoch, or else the log end
    *         offset if the leader epoch is the latest leader epoch.
    */
  def lastOffsetForLeaderEpoch(leaderEpoch: Int): EpochEndOffset = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val (epoch, offset) = leaderReplica.epochs.get.endOffsetFor(leaderEpoch)
          new EpochEndOffset(NONE, epoch, offset)
        case None =>
          new EpochEndOffset(NOT_LEADER_FOR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
    }
  }

  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(_.brokerId).toList, zkVersion)
    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topicPartition, newLeaderAndIsr,
      controllerEpoch)

    if (updateSucceeded) {
      replicaManager.recordIsrChange(topicPartition)
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      replicaManager.failedIsrUpdatesRate.mark()
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
    removeMetric("UnderMinIsr", tags)
    removeMetric("InSyncReplicasCount", tags)
    removeMetric("ReplicasCount", tags)
    removeMetric("LastStableOffsetLag", tags)
  }

  override def equals(that: Any): Boolean = that match {
    case other: Partition => partitionId == other.partitionId && topic == other.topic && isOffline == other.isOffline
    case _ => false
  }

  override def hashCode: Int =
    31 + topic.hashCode + 17 * partitionId + (if (isOffline) 1 else 0)

  override def toString(): String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AllReplicas: " + allReplicasMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString
  }
}
