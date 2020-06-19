package com.raphtory.examples.kafka.anankejson
import com.fasterxml.jackson.annotation.JsonProperty



case class VertexJson(@JsonProperty("command") val command: String,
                      @JsonProperty("vertexId") val vertexId: Long,
                      @JsonProperty("msgTime") val msgTime: Long,
                      @JsonProperty("type") val Type: String,
                      @JsonProperty("layerId") val layerId: Long,
                      @JsonProperty("properties") val properties: Map[String,Any])
{
  def isEmptyMap(): Boolean ={
    properties.isEmpty


  }


}


