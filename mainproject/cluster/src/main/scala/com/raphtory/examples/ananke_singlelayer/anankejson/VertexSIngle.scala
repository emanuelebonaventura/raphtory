package com.raphtory.examples.ananke_singlelayer.anankejson

import spray.json._

case class VertexSingle(command: String,
                      msgTime: Long,
                      vertexId: Long,
                      vertexType: String,
                      properties: Map[String,JsValue]
                     )


object VertexSingleJson extends DefaultJsonProtocol {
  implicit val vertexSingle = jsonFormat5(VertexSingle)
}

