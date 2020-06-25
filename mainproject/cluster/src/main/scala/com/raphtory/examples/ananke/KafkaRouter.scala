package com.raphtory.examples.ananke



import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.Type
import com.raphtory.core.model.communication._
import com.raphtory.examples.ananke.anankejson._
import net.liftweb.json._


class KafkaRouter(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {
  implicit val formats = DefaultFormats
  def parseTuple(record: Any): Unit = {
    val json = parse(record.asInstanceOf[String])
    json.asInstanceOf[JObject].values.get("command") match {
      case Some("EdgeAdd") =>   val edge = json.extract[EdgeJson]
                                AddNewEdge(edge)
      case Some("VertexAdd")  =>  val vertex = json.extract[VertexJson]
                                  AddNewVertex(vertex)
      case Some("VertexDelete")  =>  val vertex = json.extract[VertexJsonRemove]
                                    DeleteVertex(vertex)
      case Some("EdgeDelete") =>   val edge = json.extract[EdgeJsonRemove]
                                 DeleteEdge(edge)
      case _          =>   println("message not recognized!")
    }

  }


  def AddNewVertex(vertex:  => VertexJson): Unit = {
    if (vertex.isEmptyMap())  sendGraphUpdate(VertexAdd(vertex.msgTime, vertex.vertexId, Type(vertex.vertexType),vertex.layerId))
    else {
      var pro = Vector[Property]()
      for ((k,v) <- vertex.properties) {
       v match {
         case v : net.liftweb.json.JsonAST.JString => pro =  pro.+:( StringProperty(k,v.extract[String]))
         case v : net.liftweb.json.JsonAST.JInt  => pro = pro.+:(LongProperty(k,v.extract[Long]))
         case v : net.liftweb.json.JsonAST.JDouble  => pro = pro.+:(DoubleProperty(k,v.extract[Double]))
         case v : net.liftweb.json.JsonAST.JNull.type => pro =  pro.+:( StringProperty(k,"null"))
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

  def AddNewEdge(edge: => EdgeJson): Unit = {

    if (edge.isEmptyMap()) sendGraphUpdate(EdgeAdd(edge.msgTime, edge.srcId, edge.dstId, Type(edge.edgeType),edge.dstLayerId,edge.srcLayerId))
    else {
      var pro = Vector[Property]()
      for ((k,v) <- edge.properties) {
        v match {
          case v : net.liftweb.json.JsonAST.JString => pro =  pro.+:( StringProperty(k,v.extract[String]))
          case v : net.liftweb.json.JsonAST.JInt  => pro = pro.+:(LongProperty(k,v.extract[Long]))
          case v : net.liftweb.json.JsonAST.JDouble  => pro = pro.+:(DoubleProperty(k,v.extract[Double]))
          case v : net.liftweb.json.JsonAST.JNull.type => pro =  pro.+:( StringProperty(k,"null"))
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

  def DeleteVertex(vertex:  => VertexJsonRemove): Unit = {
      sendGraphUpdate(
        VertexDelete(
          vertex.msgTime,
          vertex.vertexId,
          vertex.layerId
        )
      )
  }


  def DeleteEdge(edge:  => EdgeJsonRemove): Unit = {
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
