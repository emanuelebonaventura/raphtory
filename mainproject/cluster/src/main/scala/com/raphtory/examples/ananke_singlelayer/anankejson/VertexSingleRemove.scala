package com.raphtory.examples.ananke_singlelayer.anankejson

import spray.json.DefaultJsonProtocol

case class VertexSingleRemove(vertexId: Long, msgTime: Long)

object VertexSingleJsonDelete extends DefaultJsonProtocol {
  implicit val vertexSingleDelete = jsonFormat2(VertexSingleRemove)
}