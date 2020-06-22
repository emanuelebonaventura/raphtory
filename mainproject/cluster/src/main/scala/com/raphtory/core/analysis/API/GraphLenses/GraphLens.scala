package com.raphtory.core.analysis.API.GraphLenses

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.ManagerCount
import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage
import java.util.concurrent.atomic.AtomicInteger

import com.raphtory.core.components.PartitionManager.Workers.ViewJob

import scala.collection.parallel.mutable.ParTrieMap

abstract class GraphLens(jobID: ViewJob, superstep: Int, storage: EntityStorage, managerCount: ManagerCount) {
  private val messages = new AtomicInteger(0)

  protected var voteCount = new AtomicInteger(0)

  def superStep() = superstep

  def getVerticesSet(): ParTrieMap[Long, Vertex] = {
    var vertices = new ParTrieMap[Long,Vertex]()
    for ((k,layer) <- storage.layers) {
      vertices = vertices.++(layer.vertices)
    }
    vertices
  }

  def getVerticesWithMessages(): ParTrieMap[Long, Vertex] = {
    var vertices = new ParTrieMap[Long,Vertex]()
    for ((k,layer) <- storage.layers) {
      vertices = vertices.++(layer.vertices.filter {
                              case (id: Long, vertex: Vertex) => vertex.multiQueue.getMessageQueue(jobID, superstep).nonEmpty
                          })
    }
    vertices
  }

  def recordMessage(sourceID: Long, vertexID: Long, data: Any) =
    messages.incrementAndGet()

  def getMessages() = messages.get

  def getVertex(v: Vertex)(implicit context: ActorContext, managerCount: ManagerCount): VertexVisitor =
    new VertexVisitor(v, jobID, superstep, this)

  def getTotalVerticesSet() = {
    var vertices = new ParTrieMap[Long,Vertex]()
    for ((k,layer) <- storage.layers) {
      vertices = vertices.++(layer.vertices)
    }
    vertices.keySet
  }

  def vertexVoted() =
    voteCount.incrementAndGet()

  def checkVotes(workerID: Int): Boolean = {
    var vertices = new ParTrieMap[Long,Vertex]()
    for ((k,layer) <- storage.layers) {
      vertices = vertices.++(layer.vertices)
    }
    vertices.size == voteCount.get
  }
}
