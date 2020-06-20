package com.raphtory.examples.ananke.anankejson

import net.liftweb.json.JsonAST.JValue


case class EdgeJson(  command: String,
                     srcId: Long,
                     dstId: Long,
                     msgTime: Long,
                      edgeType: String,
                     srcLayerId: Long,
                      dstLayerId: Long,
                      properties: Map[String,JValue]) {

  def isEmptyMap(): Boolean ={
    properties.isEmpty


  }


}