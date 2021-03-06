package com.raphtory.core.analysis.API.GraphLenses

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.ManagerCount
import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor
import com.raphtory.core.components.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage
import kamon.Kamon

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap

class WindowLens(
    viewJobOriginal: ViewJob,
    superstep: Int,
    workerID: Int,
    storage: EntityStorage,
    managerCount: ManagerCount
) extends GraphLens(viewJobOriginal, superstep, storage, managerCount) {
  var viewJobCurrent = viewJobOriginal
  val jobID = viewJobCurrent.jobID
  val timestamp = viewJobCurrent.timestamp
  val window = viewJobCurrent.window
  private var setWindow = window

  private val viewTimer = Kamon.gauge("Raphtory_View_Build_Time")
    .withTag("Partition",storage.managerID)
    .withTag("Worker",workerID)
    .withTag("JobID",jobID)
    .withTag("timestamp",timestamp)
  private val timetaken = System.currentTimeMillis()

  private var keySet: ParTrieMap[Long, Vertex] = {
    var vertices : ParTrieMap[Long,Vertex] =  ParTrieMap[Long,Vertex]()
    for ((k,layer) <- storage.layers) {
      vertices = vertices.++(layer.vertices)
    }
    vertices.filter(v => v._2.aliveAtWithWindow(timestamp, setWindow))
  }

  viewTimer.update(System.currentTimeMillis()-timetaken)

  private var TotalKeySize = 0
  private var firstCall    = true
  var timeTest             = ArrayBuffer[Long]()

  override def getVerticesSet(): ParTrieMap[Long, Vertex] = {
    if (firstCall) {
      TotalKeySize += keySet.size
      firstCall = false
    }
    keySet
  }

  private var keySetMessages: ParTrieMap[Long, Vertex] = null
  private var messageFilter                            = false

  override def getVerticesWithMessages(): ParTrieMap[Long, Vertex] = {
    if (!messageFilter) {
      keySetMessages = keySet.filter {
        case (id: Long, vertex: Vertex) => vertex.multiQueue.getMessageQueue(viewJobCurrent, superstep).nonEmpty
      }
      TotalKeySize = keySetMessages.size + TotalKeySize
      messageFilter = true
    }
    keySetMessages
  }

  override def getVertex(v: Vertex)(implicit context: ActorContext, managerCount: ManagerCount): VertexVisitor =
    new VertexVisitor(v.viewAtWithWindow(timestamp, setWindow), viewJobCurrent, superstep, this)

  def shrinkWindow(newWindowSize: Long) = {
    setWindow = newWindowSize
    keySet = keySet.filter(v => (v._2).aliveAtWithWindow(timestamp, setWindow))
    messageFilter = false
    firstCall = true
    viewJobCurrent = ViewJob(viewJobCurrent.jobID,viewJobCurrent.timestamp,newWindowSize)
    //println(s"$workerID $timestamp $newWindowSize keyset prior $x keyset after ${keySet.size}")
  }

  override def checkVotes(workerID: Int): Boolean =
    TotalKeySize == voteCount.get
}
