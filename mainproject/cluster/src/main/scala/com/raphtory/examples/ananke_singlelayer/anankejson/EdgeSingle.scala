package com.raphtory.examples.ananke_singlelayer.anankejson

import spray.json.{DefaultJsonProtocol, JsValue}

case class EdgeSingle(command: String,
                    msgTime: Long,
                    srcId: Long,
                    dstId: Long,
                    edgeType: Option[String],
                    properties: Option[Map[String,JsValue]])

object EdgeSingleJson extends DefaultJsonProtocol {
  implicit val edgeSingle = jsonFormat6(EdgeSingle.apply)
}

