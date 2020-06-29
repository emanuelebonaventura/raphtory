package com.raphtory.examples.ananke_singlelayer.anankejson

import spray.json.DefaultJsonProtocol

case class EdgeSingleRemove(srcId: Long, dstId : Long, msgTime: Long)

object EdgeSingleJsonDelete extends DefaultJsonProtocol {
  implicit val edgeSingleDelete = jsonFormat3(EdgeSingleRemove)
}