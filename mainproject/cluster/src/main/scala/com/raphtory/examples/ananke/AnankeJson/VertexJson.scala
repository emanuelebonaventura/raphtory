package com.raphtory.examples.ananke.anankejson

import net.liftweb.json.JsonAST.JValue


case class VertexJson(  command: String,
                       vertexId: Long,
                       msgTime: Long,
                        vertexType: String,
                      layerId: Long,
                       properties: Map[String,JValue])
{
  def isEmptyMap(): Boolean ={
    properties.isEmpty


  }


}


