package com.raphtory.examples.ananke_multilayer.anankejson

import spray.json._

case class VertexMulti(command: String,
                      msgTime: Long,
                      vertexId: Long,
                      vertexType: String,
                      layerId: Long = 0,
                      properties: Map[String,JsValue]
                     )


object VertexMultiJson extends DefaultJsonProtocol {
  implicit val vertexMulti = jsonFormat6(VertexMulti)
}

