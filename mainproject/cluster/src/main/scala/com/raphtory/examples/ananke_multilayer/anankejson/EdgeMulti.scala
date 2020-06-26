package com.raphtory.examples.ananke_multilayer.anankejson

import spray.json.{DefaultJsonProtocol, JsValue}

case class EdgeMulti(command: String,
                    msgTime: Long,
                    srcId: Long,
                    dstId: Long,
                    edgeType: String,
                    srcLayerId: Long = 0,
                    dstLayerId: Long = 0,
                    properties: Map[String,JsValue])

object EdgeMultiJson extends DefaultJsonProtocol {
  implicit val edgeMulti = jsonFormat8(EdgeMulti)
}

