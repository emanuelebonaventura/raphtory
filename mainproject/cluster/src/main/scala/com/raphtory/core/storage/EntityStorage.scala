package com.raphtory.core.storage

import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.Edge
import com.raphtory.core.model.graphentities.Entity
import com.raphtory.core.model.graphentities.SplitEdge
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.model.graphentities.Layer
import com.raphtory.core.utils.Utils
import kamon.Kamon

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Singleton representing the Storage for the entities
  */
//TODO add capacity function based on memory used and number of updates processed/stored in memory
//TODO What happens when an edge which has been archived gets readded

class EntityStorage(partitionID:Int,workerID: Int) {
  import com.raphtory.core.utils.Utils.checkDst
  import com.raphtory.core.utils.Utils.checkWorker
  import com.raphtory.core.utils.Utils.getManager
  import com.raphtory.core.utils.Utils.getPartition
  val debug = System.getenv().getOrDefault("DEBUG", "false").trim.toBoolean

  /**
    * Map of vertices contained in the partition
    */
  var baseLayer = Layer(0,0)
  val layers = ParTrieMap[Long, Layer]()
  layers put(baseLayer.layerID,baseLayer)


  var printing: Boolean  = true
  var managerCount: Int  = 1
  var managerID: Int     = 0
  var mediator: ActorRef = null
  var windowing: Boolean = Utils.windowing
  //stuff for compression and archiving
  var oldestTime: Long       = Long.MaxValue
  var newestTime: Long       = 0
  var windowTime: Long       = 0
  var safeWindowTime:Long    = 0
  var windowSafe:Boolean     = false
  var lastCompressedAt: Long = 0

  val vertexCount          = Kamon.counter("Raphtory_Vertex_Count").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerID)
  val localEdgeCount       = Kamon.counter("Raphtory_Local_Edge_Count").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerID)
  val copySplitEdgeCount   = Kamon.counter("Raphtory_Copy_Split_Edge_Count").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerID)
  val masterSplitEdgeCount = Kamon.counter("Raphtory_Master_Split_Edge_Count").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerID)
  val layerCount         = Kamon.counter("Raphtory_Layers_Count").withTag("actor",s"PartitionWriter_$partitionID").withTag("ID",workerID)
  //ADDING LAYER 0
  layerCount.increment()

  def timings(updateTime: Long) = {
    if (updateTime < oldestTime && updateTime > 0) oldestTime = updateTime
    if (updateTime > newestTime)
      newestTime = updateTime //this isn't thread safe, but is only an approx for the archiving
  }

  def apply(printing: Boolean, managerCount: Int, managerID: Int, mediator: ActorRef) = {
    this.printing = printing
    this.managerCount = managerCount
    this.managerID = managerID
    this.mediator = mediator
    this
  }

  def setManagerCount(count: Int) = this.managerCount = count

  def addProperties(msgTime: Long, entity: Entity, properties: Properties) =
    if (properties != null)
      properties.property.foreach {
        case StringProperty(key, value)    => entity + (msgTime, false, key, value)
        case LongProperty(key, value)      => entity + (msgTime, false, key, value)
        case DoubleProperty(key, value)    => entity + (msgTime, false, key, value)
        case ImmutableProperty(key, value) => entity + (msgTime, true, key, value)
      }
  // if the add come with some properties add all passed properties into the entity

  def vertexAdd(msgTime: Long, srcId: Long, properties: Properties = null, vertexType: Type, layerId : Long = 0): Vertex = { //Vertex add handler function
    def vertexSearch(l: Layer): Vertex = {
      val vertex: Vertex = l.vertices.get(srcId) match { //check if the vertex exists
        case Some(v) => //if it does
          v revive msgTime //add the history point
          v
        case None => //if it does not exist
          val v = new Vertex(msgTime, srcId, initialValue = true, storage = this) //create a new vertex
          vertexCount.increment()
          if (!(vertexType == null)) v.setType(vertexType.name)
          l.vertices put(srcId, v) //put it in the map
          v
      }
      addProperties(msgTime, vertex, properties)
      vertex
    }


    val vertex: Vertex = layers.get(layerId) match {
      case Some(l) => vertexSearch(l)
      case None => {
        val l = new Layer(msgTime, layerId)
        layerCount.increment()
        layers put(layerId, l)
        vertexSearch(l)
      }
    }
    vertex
  }

  def getVertexOrPlaceholder(msgTime: Long, id: Long, layerId: Long = 0): Vertex = {
    def vertexSearch(layer: Layer): Vertex = {
      layer.vertices.get(id) match {
        case Some(vertex) => vertex
        case None =>
          val vertex = new Vertex(msgTime, id, initialValue = true, this)
          vertexCount.increment()
          layer.vertices put(id, vertex)
          vertex wipe()
          vertex
      }
    }


    layers.get(layerId) match {
      case Some(layer) => {val vertex = vertexSearch(layer)
                          vertex
                        }
      case None => {
        val l = new Layer(msgTime, layerId)
        layerCount.increment()
        layers put(layerId, l)
        val vertex = vertexSearch(l)
        vertex
      }
    }
  }

  def vertexWorkerRequest(msgTime: Long, dstID: Long, srcID: Long, edge: Edge, present: Boolean,routerID:String,routerTime:Int) = {
    val dstVertex = vertexAdd(msgTime, dstID, vertexType = null, layerId = edge.getDstLayerId) //if the worker creating an edge does not deal with the destination
    if (!present) {
      dstVertex.incrementEdgesRequiringSync()
      dstVertex addIncomingEdge edge // do the same for the destination node
      mediator ! DistributedPubSubMediator.Send( //if this edge is new
              getManager(srcID, managerCount),
              DstResponseFromOtherWorker(msgTime, srcID, dstID, dstVertex.removeList, routerID, routerTime, edge.getSrcLayerId),
              false
      )
    }
    else
      mediator ! DistributedPubSubMediator.Send( //if this edge is not new we just need to ack
        getManager(srcID, managerCount),
        EdgeSyncAck(msgTime, routerID, routerTime),
        false
      )
  }

  def vertexWipeWorkerRequest(msgTime: Long, dstID: Long, srcID: Long, edge: Edge, present: Boolean,routerID:String,routerTime:Int) = {
    val dstVertex = getVertexOrPlaceholder(msgTime, dstID,edge.getDstLayerId) // if the worker creating an edge does not deal with do the same for the destination ID
    if (!present) {
      dstVertex.incrementEdgesRequiringSync()
      dstVertex addIncomingEdge edge // do the same for the destination node
      mediator ! DistributedPubSubMediator.Send( //as it is new respond with teh deletions
              getManager(srcID, managerCount),
              DstResponseFromOtherWorker(msgTime, srcID, dstID, dstVertex.removeList,routerID,routerTime, edge.getSrcLayerId),
              false
      )
    }
    else
      mediator ! DistributedPubSubMediator.Send( //if this edge is not new we just need to ack
        getManager(srcID, managerCount),
        EdgeSyncAck(msgTime, routerID, routerTime),
        false
      )
  }

  def vertexWorkerRequestEdgeHandler(
      msgTime: Long,
      srcID: Long,
      dstID: Long,
      layerId : Long = 0,
      removeList: mutable.TreeMap[Long, Boolean]
  ): Unit =
    getVertexOrPlaceholder(msgTime, srcID,layerId = layerId).getOutgoingEdge(dstID) match {
      case Some(edge) => edge killList removeList //add the dst removes into the edge
      case None       => println("Edge not found")
    }

  def vertexRemoval(msgTime: Long, srcId: Long,routerID:String,routerTime:Int,layerId : Long = 0):Int = {
    def vertexSearch(layer: Layer): Vertex ={
      val vertex: Vertex = layer.vertices.get(srcId) match {
        case Some(v) =>
          v kill msgTime
          v
        case None => //if the removal has arrived before the creation
          val v = new Vertex(msgTime, srcId, initialValue = false, this) //create a placeholder
          vertexCount.increment()
          layer.vertices put (srcId, v) //add it to the map
          v
      }
      vertex
    }


    val vertex: Vertex = layers.get(layerId) match {
      case Some(l) => vertexSearch(l)
      case None => {
        val l = new Layer(msgTime, layerId)
        layerCount.increment()
        layers put(layerId, l)
        vertexSearch(l)
      }
    }

    //todo decide with hamza which one to use

//     vertex.incomingEdges.values.foreach {
//      case edge @ (remoteEdge: SplitEdge) =>
//        edge kill msgTime
//        mediator ! DistributedPubSubMediator.Send(
//                getManager(remoteEdge.getSrcId, managerCount),
//                ReturnEdgeRemoval(msgTime, remoteEdge.getSrcId, remoteEdge.getDstId,routerID,routerTime),
//                false
//        ) //inform the other partition to do the same
//      case edge => //if it is a local edge -- opperated by the same worker, therefore we can perform an action -- otherwise we must inform the other local worker to handle this
//        if (edge.getWorkerID == workerID) edge kill msgTime
//        else
//          mediator ! DistributedPubSubMediator.Send(
//                  getManager(edge.getSrcId, managerCount),
//                  EdgeRemoveForOtherWorker(msgTime, edge.getSrcId, edge.getDstId,routerID,routerTime),
//                  false
//          ) //
//    }
//    vertex.outgoingEdges.values.foreach {
//      case edge @ (remoteEdge: SplitEdge) =>
//        edge kill msgTime //outgoing edge always opperated by the same worker, therefore we can perform an action
//        mediator ! DistributedPubSubMediator.Send(
//                getManager(edge.getDstId, managerCount),
//                RemoteEdgeRemovalFromVertex(msgTime, remoteEdge.getSrcId, remoteEdge.getDstId,routerID,routerTime),
//                false
//        )
//      case edge =>
//        edge kill msgTime //outgoing edge always opperated by the same worker, therefore we can perform an action
//    }
    val incomingCount = vertex.incomingEdges.map(edge => {
      edge._2 match {
        case edge@(remoteEdge: SplitEdge) =>
          edge kill msgTime
          mediator ! DistributedPubSubMediator.Send(
            getManager(remoteEdge.getSrcId, managerCount),
            ReturnEdgeRemoval(msgTime, remoteEdge.getSrcId, remoteEdge.getDstId, routerID, routerTime,edge.getSrcLayerId),
            false
          ) //inform the other partition to do the same
          1
        case edge => //if it is a local edge -- opperated by the same worker, therefore we can perform an action -- otherwise we must inform the other local worker to handle this
          if (edge.getWorkerID == workerID) {
            edge kill msgTime
            0
          }
          else {
            mediator ! DistributedPubSubMediator.Send(
              getManager(edge.getSrcId, managerCount),
              EdgeRemoveForOtherWorker(msgTime, edge.getSrcId, edge.getDstId, routerID, routerTime,edge.getSrcLayerId),
              false
            ) //
            1
          }
      }
    })
    val outgoingCount = vertex.outgoingEdges.map (edge=>{
      edge._2 match {
        case edge@(remoteEdge: SplitEdge) =>
          edge kill msgTime //outgoing edge always opperated by the same worker, therefore we can perform an action
          mediator ! DistributedPubSubMediator.Send(
            getManager(edge.getDstId, managerCount),
            RemoteEdgeRemovalFromVertex(msgTime, remoteEdge.getSrcId, remoteEdge.getDstId, routerID, routerTime,edge.getDstLayerId),
            false
          )
          1
        case edge =>
          edge kill msgTime //outgoing edge always opperated by the same worker, therefore we can perform an action
          0
      }
    })
    if(!(incomingCount.sum+outgoingCount.sum == vertex.getEdgesRequringSync()))
      println(s"Incorrect ${incomingCount.sum+outgoingCount.sum} ${vertex.getEdgesRequringSync()}")
    incomingCount.sum+outgoingCount.sum
  }

  /**
    * Edges Methods
    */
  def edgeAdd(msgTime: Long, srcId: Long, dstId: Long,routerID:String,routerTime:Int, properties: Properties = null, edgeType: Type,srcLayerId : Long = 0, dstLayerId : Long = 0 ):Boolean = {
    val local      = checkDst(dstId, managerCount, managerID)     //is the dst on this machine
    val sameWorker = checkWorker(dstId, managerCount, workerID)   // is the dst handled by the same worker
    val srcVertex  = vertexAdd(msgTime, srcId, vertexType = null, layerId = srcLayerId) // create or revive the source ID

    var present    = false //if the vertex is new or not -- decides what update is sent when remote and if to add the source/destination removals
    var edge: Edge = null
    srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) => //retrieve the edge if it exists
        edge = e
        present = true
      case None => //if it does not
        if (local) {
          edge = new Edge(workerID, msgTime, srcId, dstId,srcLayerId,dstLayerId, initialValue = true, this) //create the new edge, local or remote
          localEdgeCount.increment()
        } else {
          edge = new SplitEdge(workerID, msgTime, srcId, dstId,srcLayerId,dstLayerId, initialValue = true, getPartition(dstId, managerCount), this)
          masterSplitEdgeCount.increment()
        }
        if (!(edgeType == null)) edge.setType(edgeType.name)
        srcVertex.addOutgoingEdge(edge) //add this edge to the vertex
    }
    if (local && srcId != dstId)
      if (sameWorker) { //if the dst is handled by the same worker
        val dstVertex = vertexAdd(msgTime, dstId, vertexType = null,layerId = dstLayerId) // do the same for the destination ID
        if (!present) {
          dstVertex addIncomingEdge (edge)   // add it to the dst as would not have been seen
          edge killList dstVertex.removeList //add the dst removes into the edge
        }
      } else // if it is a different worker, ask that other worker to complete the dst part of the edge
        mediator ! DistributedPubSubMediator
          .Send(getManager(dstId, managerCount), DstAddForOtherWorker(msgTime, dstId, srcId, edge, present, routerID:String, routerTime:Int), true)

    if (present) {
      edge revive msgTime //if the edge was previously created we need to revive it
      if (!local)         // if it is a remote edge we
        mediator ! DistributedPubSubMediator.Send(
                getManager(dstId, managerCount),
                RemoteEdgeAdd(msgTime, srcId, dstId, srcLayerId,dstLayerId,properties, edgeType, routerID:String, routerTime:Int),
                false
        )    // inform the partition dealing with the destination node*/
    } else { // if this is the first time we have seen the edge
      val deaths = srcVertex.removeList //we extract the removals from the src
      edge killList deaths // add them to the edge
      if (!local)          // and if not local sync with the other partition
        mediator ! DistributedPubSubMediator.Send(
                getManager(dstId, managerCount),
                RemoteEdgeAddNew(msgTime, srcId, dstId, srcLayerId,dstLayerId,properties, deaths, edgeType, routerID:String, routerTime:Int),
                false
        )
    }
    addProperties(msgTime, edge, properties)
    if(!local && !present) //if its not fully local and is new then increment the count for edges requireing a watermark count
      srcVertex.incrementEdgesRequiringSync()
    local && sameWorker //return if the edge has no sync
  }

  def remoteEdgeAddNew(
      msgTime: Long,
      srcId: Long,
      dstId: Long,
      srcLayerId:Long = 0,
      dstLayerId:Long = 0,
      properties: Properties,
      srcDeaths: mutable.TreeMap[Long, Boolean],
      edgeType: Type,
      routerID:String,
      routerTime:Int
  ): Unit = {
    val dstVertex = vertexAdd(msgTime, dstId, vertexType = null, layerId = dstLayerId) //create or revive the destination node
    val edge = new SplitEdge(workerID, msgTime, srcId, dstId,srcLayerId,dstLayerId, initialValue = true, getPartition(srcId, managerCount), this)
    copySplitEdgeCount.increment()
    dstVertex addIncomingEdge (edge) //add the edge to the associated edges of the destination node
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths    // pass destination node death lists to the edge
    addProperties(msgTime, edge, properties)
    dstVertex.incrementEdgesRequiringSync()
    if (!(edgeType == null)) edge.setType(edgeType.name)
    mediator ! DistributedPubSubMediator
      .Send(getManager(srcId, managerCount), RemoteReturnDeaths(msgTime, srcId, dstId, deaths, routerID, routerTime,srcLayerId), false)
  }

  def remoteEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, srcLayerId : Long =0,dstLayerId : Long =0,properties: Properties = null, edgeType: Type,routerID:String,routerTime:Int): Unit = {
    val dstVertex = vertexAdd(msgTime, dstId, vertexType = null,layerId = dstLayerId) // revive the destination node
    dstVertex.getIncomingEdge(srcId) match {
      case Some(edge) =>
        edge revive msgTime //revive the edge
        addProperties(msgTime, edge, properties)
      case None => /*todo should this happen */
    }
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount), EdgeSyncAck(msgTime, routerID, routerTime), true)
  }

  def edgeRemoval(msgTime: Long, srcId: Long, dstId: Long,srcLayerId : Long = 0, dstLayerId : Long = 0, routerID: String, routerTime: Int): Boolean = {
    val local      = checkDst(dstId, managerCount, managerID)
    val sameWorker = checkWorker(dstId, managerCount, workerID) // is the dst handled by the same worker

    var present           = false
    var edge: Edge        = null
    val srcVertex: Vertex = getVertexOrPlaceholder(msgTime, srcId, layerId = srcLayerId)

    srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) =>
        edge = e
        present = true
      case None =>
        if (local) {
          localEdgeCount.increment()
          edge = new Edge(workerID, msgTime, srcId, dstId,srcLayerId,dstLayerId, initialValue = false, this)
        } else {
          masterSplitEdgeCount.increment()
          edge = new SplitEdge(workerID, msgTime, srcId, dstId,srcLayerId,dstLayerId, initialValue = false, getPartition(dstId, managerCount), this)
        }
        srcVertex addOutgoingEdge (edge) // add the edge to the associated edges of the source node
    }
    if (local && srcId != dstId)
      if (sameWorker) { //if the dst is handled by the same worker
        val dstVertex = getVertexOrPlaceholder(msgTime, dstId,layerId = dstLayerId) // do the same for the destination ID
        if (!present) {
          dstVertex addIncomingEdge (edge)   // do the same for the destination node
          edge killList dstVertex.removeList //add the dst removes into the edge
        }
      } else // if it is a different worker, ask that other worker to complete the dst part of the edge
        mediator ! DistributedPubSubMediator
          .Send(getManager(dstId, managerCount), DstWipeForOtherWorker(msgTime, dstId, srcId, edge, present, routerID, routerTime), true)

    if (present) {
      edge kill msgTime
      if (!local)
        mediator ! DistributedPubSubMediator.Send(
                getManager(dstId, managerCount),
                RemoteEdgeRemoval(msgTime, srcId, dstId, routerID, routerTime,dstLayerId),
                false
        ) // inform the partition dealing with the destination node
    } else {
      val deaths = srcVertex.removeList
      edge killList deaths
      if (!local)
        mediator ! DistributedPubSubMediator
          .Send(getManager(dstId, managerCount), RemoteEdgeRemovalNew(msgTime, srcId, dstId, srcLayerId,dstLayerId,deaths, routerID, routerTime), false)
    }
    if(!local && !present) //if its not fully local and is new then increment the count for edges requireing a watermark count
      srcVertex.incrementEdgesRequiringSync()
    local && sameWorker
  }

  def returnEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long,routerID:String,routerTime:Int,layerId : Long = 0): Unit = { //for the source getting an update about deletions from a remote worker
    getVertexOrPlaceholder(msgTime, srcId,layerId = layerId).getOutgoingEdge(dstId) match {
      case Some(edge) => edge kill msgTime
      case None       => //todo should this happen
    }
    mediator ! DistributedPubSubMediator.Send( // ack the destination holder that this is all sorted
      getManager(dstId, managerCount),
      VertexRemoveSyncAck(msgTime, routerID, routerTime),
      false
    )
  }

  def edgeRemovalFromOtherWorker(msgTime: Long, srcID: Long, dstID: Long,routerID:String,routerTime:Int,layerId : Long = 0) = {
    getVertexOrPlaceholder(msgTime, srcID,layerId = layerId).getOutgoingEdge(dstID) match {
      case Some(edge) => edge kill msgTime
      case None       => //todo should this happen?
    }
    mediator ! DistributedPubSubMediator.Send( // ack the destination holder that this is all sorted
      getManager(dstID, managerCount),
      VertexRemoveSyncAck(msgTime, routerID, routerTime),
      false
    )
  }

  def remoteEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long,routerID:String,routerTime:Int,layerId : Long = 0): Unit = {
    val dstVertex = getVertexOrPlaceholder(msgTime, dstId, layerId = layerId)
    dstVertex.getIncomingEdge(srcId) match {
      case Some(e) => e kill msgTime
      case None    => println(s"Worker ID $workerID Manager ID $managerID")
    }
    mediator ! DistributedPubSubMediator.Send( //if this edge is not new we just need to ack
      getManager(srcId, managerCount),
      EdgeSyncAck(msgTime, routerID, routerTime),
      false
    )
  }
  def remoteEdgeRemovalFromVertex(msgTime: Long, srcId: Long, dstId: Long,routerID:String,routerTime:Int,layerId : Long = 0): Unit = {
    val dstVertex = getVertexOrPlaceholder(msgTime, dstId, layerId = layerId)
    dstVertex.getIncomingEdge(srcId) match {
      case Some(e) => e kill msgTime
      case None    => println(s"Worker ID $workerID Manager ID $managerID")
    }
    mediator ! DistributedPubSubMediator.Send( //if this edge is not new we just need to ack
      getManager(srcId, managerCount),
      VertexRemoveSyncAck(msgTime, routerID, routerTime),
      false
    )
  }

  def remoteEdgeRemovalNew(msgTime: Long, srcId: Long, dstId: Long,srcLayerId : Long = 0,dstLayerId : Long = 0, srcDeaths: mutable.TreeMap[Long, Boolean],routerID:String,routerTime:Int): Unit = {
    val dstVertex = getVertexOrPlaceholder(msgTime, dstId, layerId = dstLayerId)
    dstVertex.incrementEdgesRequiringSync()
    copySplitEdgeCount.increment()
    val edge = new SplitEdge(workerID, msgTime, srcId, dstId,srcLayerId,dstLayerId, initialValue = false, getPartition(srcId, managerCount), this)
    dstVertex addIncomingEdge (edge) //add the edge to the destination nodes associated list
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths    // pass destination node death lists to the edge
    mediator ! DistributedPubSubMediator
      .Send(getManager(srcId, managerCount), RemoteReturnDeaths(msgTime, srcId, dstId, deaths,routerID,routerTime,srcLayerId), false)
  }

  def remoteReturnDeaths(msgTime: Long, srcId: Long, dstId: Long, dstDeaths: mutable.TreeMap[Long, Boolean],layerId : Long = 0): Unit = {
    if (printing) println(s"Received deaths for $srcId --> $dstId from ${getManager(dstId, managerCount)}")
    getVertexOrPlaceholder(msgTime, srcId, layerId = layerId).getOutgoingEdge(dstId) match {
      case Some(edge) => edge killList dstDeaths
      case None       => /*todo Should this happen*/
    }
  }
}
