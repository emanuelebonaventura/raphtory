package com.raphtory.examples.ananke.anankejson

import spray.json.DefaultJsonProtocol

case class EdgeRemove(srcId: Long, dstId : Long, msgTime: Long, srcLayerId: Option[Long], dstLayerId: Option[Long])

object EdgeMultiJsonDelete extends DefaultJsonProtocol {
  implicit val edgeMultiDelete = jsonFormat5(EdgeRemove.apply)
}