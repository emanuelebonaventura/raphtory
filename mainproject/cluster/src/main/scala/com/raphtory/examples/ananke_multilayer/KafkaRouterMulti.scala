package com.raphtory.examples.ananke_multilayer



import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication._
import com.raphtory.examples.ananke_multilayer.anankejson._
import VertexMultiJson._
import EdgeMultiJson._
import VertexMultiJsonDelete._
import EdgeMultiJsonDelete._
import spray.json._



class KafkaRouterMulti(override val routerId: Int, override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {


  def parseTuple(record: Any): Unit = {
    val jsonString = record.asInstanceOf[String]
    val json = jsonString.parseJson
    if (jsonString.contains("VertexAdd")) AddNewVertex(json.convertTo[VertexMulti])
    else if (jsonString.contains("EdgeAdd")) AddNewEdge(json.convertTo[EdgeMulti])
    else if (jsonString.contains("VertexDelete")) DeleteVertex(json.convertTo[VertexMultiDelete])
    else if (jsonString.contains("EdgeDelete")) DeleteEdge(json.convertTo[EdgeMultiDelete])

  }

  def AddNewVertex(vertex:  => VertexMulti): Unit = {

    if (vertex.properties.isEmpty) sendGraphUpdate(VertexAdd(vertex.msgTime,vertex.vertexId,Type(vertex.vertexType),vertex.layerId))
    else {
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
               vertex.layerId
             )
           )
         }
       }


  def AddNewEdge(edge: => EdgeMulti): Unit = {

    if (edge.properties.isEmpty) sendGraphUpdate(EdgeAdd(edge.msgTime, edge.srcId, edge.dstId, Type(edge.edgeType),edge.dstLayerId,edge.srcLayerId))
    else {
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
          edge.srcLayerId,
          edge.dstLayerId
        )
      )

    }
  }

  def DeleteVertex(vertex:  => VertexMultiDelete): Unit = {
      sendGraphUpdate(
        VertexDelete(
          vertex.msgTime,
          vertex.vertexId,
          vertex.layerId
        )
      )
  }


  def DeleteEdge(edge:  => EdgeMultiDelete): Unit = {
    sendGraphUpdate(
      EdgeDelete(
        edge.msgTime,
        edge.srcId,
        edge.dstId,
        edge.srcLayerId,
        edge.dstLayerId
      )
    )
  }



}
