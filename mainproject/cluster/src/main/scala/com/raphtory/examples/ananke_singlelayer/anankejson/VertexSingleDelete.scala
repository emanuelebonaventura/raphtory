package com.raphtory.examples.ananke_singlelayer.anankejson

import spray.json.DefaultJsonProtocol

case class VertexSingleDelete(vertexId: Long, msgTime: Long)

object VertexSingleJsonDelete extends DefaultJsonProtocol {
  implicit val vertexSingleDelete = jsonFormat2(VertexSingleDelete)
}