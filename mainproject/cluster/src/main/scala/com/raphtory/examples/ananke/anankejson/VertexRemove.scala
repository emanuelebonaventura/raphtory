package com.raphtory.examples.ananke.anankejson

import spray.json.DefaultJsonProtocol

case class VertexRemove(vertexId: Long, msgTime: Long, layerId: Option[Long])

object VertexMultiJsonDelete extends DefaultJsonProtocol {
  implicit val vertexMultiDelete = jsonFormat3(VertexRemove.apply)
}