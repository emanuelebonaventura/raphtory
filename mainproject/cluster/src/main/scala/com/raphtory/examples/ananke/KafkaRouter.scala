package com.raphtory.examples.ananke



import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication._
import com.raphtory.examples.ananke.anankejson._
import VertexMultiJson._
import EdgeMultiJson._
import VertexMultiJsonDelete._
import EdgeMultiJsonDelete._
import spray.json._



class KafkaRouter(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {


  def parseTuple(record: Any): Unit = {

    val jsonString = record.asInstanceOf[String]
    val json = jsonString.parseJson
    if (jsonString.contains("VertexAdd")) AddNewVertex(json.convertTo[Vertex])
    else if (jsonString.contains("EdgeAdd")) AddNewEdge(json.convertTo[Edge])
    else if (jsonString.contains("VertexDelete")) DeleteVertex(json.convertTo[VertexRemove])
    else if (jsonString.contains("EdgeDelete")) DeleteEdge(json.convertTo[EdgeRemove])

  }

  def AddNewVertex(vertex:  => Vertex): Unit = {
    if (vertex.properties.isEmpty) sendGraphUpdate(VertexAdd(vertex.msgTime, vertex.vertexId, Type(vertex.vertexType.getOrElse(null)), vertex.layerId.getOrElse(0)))
    else {
      var pro = Vector[Property]()
      vertex.properties getOrElse Map() foreach {
        case (k, v) => {
          v match {
            case v: spray.json.JsString => pro = pro.+:(StringProperty(k, v.toString()))
            case v: spray.json.JsNumber => if (v.toString().contains(".")) pro = pro.+:(DoubleProperty(k, v.toString().toDouble))
            else pro = pro.+:(LongProperty(k, v.toString().toLong))
            case v: spray.json.JsNull.type => pro = pro.+:(StringProperty(k, v.toString()))
            case _ => println("No type found!")
          }

        }
        case _ => println("error")

      }
          val properties = Properties(pro: _*)
          sendGraphUpdate(
            VertexAddWithProperties(
              vertex.msgTime,
              vertex.vertexId,
              properties,
              Type(vertex.vertexType.getOrElse(null)),
              vertex.layerId.getOrElse(0)
            )
          )
      }

  }

  def AddNewEdge(edge: => Edge): Unit = {

    if (edge.properties.isEmpty) sendGraphUpdate(EdgeAdd(edge.msgTime, edge.srcId, edge.dstId, Type(edge.edgeType.getOrElse(null)),edge.dstLayerId.getOrElse(0),edge.srcLayerId.getOrElse(0)))
    else {
      var pro = Vector[Property]()
      edge.properties getOrElse Map() foreach {
        case (k, v) => {
          v match {
            case v: spray.json.JsString => pro = pro.+:(StringProperty(k, v.toString()))
            case v: spray.json.JsNumber => if (v.toString().contains(".")) pro = pro.+:(DoubleProperty(k, v.toString().toDouble))
            else pro = pro.+:(LongProperty(k, v.toString().toLong))
            case v: spray.json.JsNull.type => pro = pro.+:(StringProperty(k, v.toString()))
            case _ => println("No type found!")
          }

        }
        case _ => println("error")
      }
      val properties =  Properties(pro : _*)
      sendGraphUpdate(
        EdgeAddWithProperties(
          edge.msgTime,
          edge.srcId,
          edge.dstId,
          properties,
          Type(edge.edgeType.getOrElse(null)),
          edge.srcLayerId.getOrElse(0),
          edge.dstLayerId.getOrElse(0)
        )
      )

    }
  }

  def DeleteVertex(vertex:  => VertexRemove): Unit = {
      sendGraphUpdate(
        VertexDelete(
          vertex.msgTime,
          vertex.vertexId,
          vertex.layerId.getOrElse(0)
        )
      )
  }


  def DeleteEdge(edge:  => EdgeRemove): Unit = {
    sendGraphUpdate(
      EdgeDelete(
        edge.msgTime,
        edge.srcId,
        edge.dstId,
        edge.srcLayerId.getOrElse(0),
        edge.dstLayerId.getOrElse(0)
      )
    )
  }
}
