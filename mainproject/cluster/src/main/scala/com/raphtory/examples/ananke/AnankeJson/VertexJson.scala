package com.raphtory.examples.kafka.anankejson




case class VertexJson(  command: String,
                       vertexId: Long,
                       msgTime: Long,
                       Type: String,
                      layerId: Long,
                       properties: Map[String,Any])
{
  def isEmptyMap(): Boolean ={
    properties.isEmpty


  }


}


