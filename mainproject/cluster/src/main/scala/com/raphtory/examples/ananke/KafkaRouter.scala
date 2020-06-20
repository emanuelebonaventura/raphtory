package com.raphtory.examples.kafka



import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.Type
import com.raphtory.core.model.communication._
import net.liftweb.json._

import com.raphtory.examples.kafka.anankejson.VertexJson
import com.raphtory.examples.kafka.anankejson.EdgeJson



class KafkaRouter(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {

  def parseTuple(record: Any): Unit = {

    implicit val formats = DefaultFormats
    val json = parse(record.asInstanceOf[String])
    println(json)

       "s" match {
      case "EdgeAdd" =>   val edge = null
                          AddNewEdge(edge)
      case "VertexAdd"  =>  val vertex = null
                            AddNewVertex(vertex)
      case _          =>   println("message not recognized!")
    }

  }


  def AddNewVertex(vertex:  => VertexJson): Unit = {
    if (vertex.isEmptyMap())  sendGraphUpdate(VertexAdd(vertex.msgTime, vertex.vertexId, Type(vertex.Type)))
    else {
      var pro = List[Property]()
      for ((k,v) <- vertex.properties) {
        v match {
          case v : java.lang.String => pro =  pro.+:( StringProperty(k,v.asInstanceOf[String]))
          case v : java.lang.Long  => pro = pro.+:(LongProperty(k,v.asInstanceOf[Long]))
          case v : java.lang.Integer  => pro = pro.+:(LongProperty(k,v.asInstanceOf[Int]))
          case v : java.lang.Double => pro = pro.+:(DoubleProperty(k,v.asInstanceOf[Double]))
          case _ => println("No type found!")
        }
      }

      val properties =  Properties(pro : _*)
      sendGraphUpdate(
        VertexAddWithProperties(
          vertex.msgTime,
          vertex.vertexId,
          properties,
          Type(vertex.Type)
        )
      )
    }
  }

  def AddNewEdge(edge: => EdgeJson): Unit = {

    if (edge.isEmptyMap()) sendGraphUpdate(EdgeAdd(edge.msgTime, edge.srcId, edge.dstId, Type(edge.Type)))
    else {
      var pro = List[Property]()
      for ((k,v) <- edge.properties) {
        v match {
          case v : java.lang.String => pro =  pro.+:( StringProperty(k,v.asInstanceOf[String]))
          case v : java.lang.Long  => pro = pro.+:(LongProperty(k,v.asInstanceOf[Long]))
          case v : java.lang.Integer  => pro = pro.+:(LongProperty(k,v.asInstanceOf[Int]))
          case v : java.lang.Double => pro = pro.+:(DoubleProperty(k,v.asInstanceOf[Double]))
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
          Type(edge.Type),
          edge.srcLayerId,
          edge.dstLayerId
        )
      )

    }
  }
}
