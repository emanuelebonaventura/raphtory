package com.raphtory.examples.ananke_multilayer.anankejson

import spray.json.DefaultJsonProtocol

case class VertexMultiDelete(vertexId: Long, msgTime: Long, layerId: Long = 0)

object VertexMultiJsonDelete extends DefaultJsonProtocol {
  implicit val vertexMultiDelete = jsonFormat3(VertexMultiDelete)
}