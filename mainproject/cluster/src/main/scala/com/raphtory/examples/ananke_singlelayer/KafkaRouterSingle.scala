package com.raphtory.examples.ananke_singlelayer

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication._
import com.raphtory.examples.ananke_singlelayer.anankejson._
import VertexSingleJson._
import EdgeSingleJson._
import VertexSingleJsonDelete._
import EdgeSingleJsonDelete._
import spray.json._



class KafkaRouterSingle(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {


  def parseTuple(record: Any): Unit = {
    val jsonString = record.asInstanceOf[String]
    val json = jsonString.parseJson
    if (jsonString.contains("VertexAdd")) AddNewVertex(json.convertTo[VertexSingle])
    else if (jsonString.contains("EdgeAdd")) AddNewEdge(json.convertTo[EdgeSingle])
    else if (jsonString.contains("VertexDelete")) DeleteVertex(json.convertTo[VertexSingleRemove])
    else if (jsonString.contains("EdgeDelete")) DeleteEdge(json.convertTo[EdgeSingleRemove])

  }

  def AddNewVertex(vertex:  => VertexSingle): Unit = {
    if (vertex.properties.isEmpty) sendGraphUpdate(VertexAdd(vertex.msgTime, vertex.vertexId, Type(vertex.vertexType.getOrElse(null))))
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
        )
      )
    }
  }


  def AddNewEdge(edge: => EdgeSingle): Unit = {
    if (edge.properties.isEmpty) sendGraphUpdate(EdgeAdd(edge.msgTime, edge.srcId, edge.dstId, Type(edge.edgeType.getOrElse(null))))
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

      val properties = Properties(pro: _*)
      sendGraphUpdate(
        EdgeAddWithProperties(
          edge.msgTime,
          edge.srcId,
          edge.dstId,
          properties,
          Type(edge.edgeType.getOrElse(null)),
        )
      )
    }
  }


  def DeleteVertex(vertex:  => VertexSingleRemove): Unit = {
      sendGraphUpdate(
        VertexDelete(
          vertex.msgTime,
          vertex.vertexId
        )
      )
  }


  def DeleteEdge(edge:  => EdgeSingleRemove): Unit = {
    sendGraphUpdate(
      EdgeDelete(
        edge.msgTime,
        edge.srcId,
        edge.dstId
      )
    )
  }



}
