package com.raphtory.examples.ananke.anankejson

import spray.json.{DefaultJsonProtocol, JsValue}

case class Edge(command: String,
                    msgTime: Long,
                    srcId: Long,
                    dstId: Long,
                    edgeType: Option[String],
                     srcLayerId: Option[Long],
                    dstLayerId: Option[Long],
                     properties: Option[Map[String,JsValue]])

object EdgeMultiJson extends DefaultJsonProtocol {
  implicit val edgeMulti = jsonFormat8(Edge.apply)
}

