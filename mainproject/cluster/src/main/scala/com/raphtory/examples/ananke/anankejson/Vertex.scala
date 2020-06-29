package com.raphtory.examples.ananke.anankejson

import spray.json._

case class Vertex(command: String,
                      msgTime: Long,
                      vertexId: Long,
                      vertexType: Option[String],
                      layerId: Option[Long],
                      properties: Option[Map[String,JsValue]]
                     )


object VertexMultiJson extends DefaultJsonProtocol {
  implicit val vertexMulti = jsonFormat6(Vertex.apply)
}

