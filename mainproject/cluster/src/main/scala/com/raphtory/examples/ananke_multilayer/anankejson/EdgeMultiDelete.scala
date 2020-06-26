package com.raphtory.examples.ananke_multilayer.anankejson

import spray.json.DefaultJsonProtocol

case class EdgeMultiDelete(srcId: Long, dstId : Long, msgTime: Long, srcLayerId: Long = 0, dstLayerId: Long = 0)

object EdgeMultiJsonDelete extends DefaultJsonProtocol {
  implicit val edgeMultiDelete = jsonFormat5(EdgeMultiDelete)
}