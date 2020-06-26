package com.raphtory.examples.ananke_singlelayer



import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication._
import com.raphtory.examples.ananke_singlelayer.anankejson._
import VertexSingleJson._
import EdgeSingleJson._
import VertexSingleJsonDelete._
import EdgeSingleJsonDelete._
import spray.json._



class KafkaRouterMulti(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {


  def parseTuple(record: Any): Unit = {
    val jsonString = record.asInstanceOf[String]
    val json = jsonString.parseJson
    if (jsonString.contains("VertexAdd")) AddNewVertex(json.convertTo[VertexSingle])
    else if (jsonString.contains("EdgeAdd")) AddNewEdge(json.convertTo[EdgeSingle])
    else if (jsonString.contains("VertexDelete")) DeleteVertex(json.convertTo[VertexSingleDelete])
    else if (jsonString.contains("EdgeDelete")) DeleteEdge(json.convertTo[EdgeSingleDelete])

  }

  def AddNewVertex(vertex:  => VertexSingle): Unit = {

      var pro = Vector[Property]()
      for ((k, v) <- vertex.properties) {
        v match {
         case v : spray.json.JsString => pro =  pro.+:( StringProperty(k,v.toString()))
         case v : spray.json.JsNumber => if (v.toString().contains("."))  pro = pro.+:(DoubleProperty(k,v.toString().toDouble))
                                         else  pro = pro.+:(LongProperty(k,v.toString().toLong))
         case v : spray.json.JsNull.type => pro =  pro.+:( StringProperty(k,v.toString()))
         case _ => println("No type found!")
        }
      }
           val properties =  Properties(pro : _*)
           sendGraphUpdate(
             VertexAddWithProperties(
               vertex.msgTime,
               vertex.vertexId,
               properties,
               Type(vertex.vertexType),
             )
           )
  }


  def AddNewEdge(edge: => EdgeSingle): Unit = {

      var pro = Vector[Property]()
      for ((k,v) <- edge.properties) {
        v match {
          case v : spray.json.JsString => pro =  pro.+:( StringProperty(k,v.toString()))
          case v : spray.json.JsNumber => if (v.toString().contains("."))  pro = pro.+:(DoubleProperty(k,v.toString().toDouble))
          else  pro = pro.+:(LongProperty(k,v.toString().toLong))
          case v : spray.json.JsNull.type => pro =  pro.+:( StringProperty(k,v.toString()))
          case _ => println("No type found!")
        }
      }

      val properties =  Properties(pro : _*)
      sendGraphUpdate(
        EdgeAddWithProperties(
          edge.msgTime,
          edge.srcId,
          edge.dstId,
          properties,
          Type(edge.edgeType),
        )
      )

  }


  def DeleteVertex(vertex:  => VertexSingleDelete): Unit = {
      sendGraphUpdate(
        VertexDelete(
          vertex.msgTime,
          vertex.vertexId
        )
      )
  }


  def DeleteEdge(edge:  => EdgeSingleDelete): Unit = {
    sendGraphUpdate(
      EdgeDelete(
        edge.msgTime,
        edge.srcId,
        edge.dstId
      )
    )
  }



}
