package com.raphtory.examples.kafka.anankejson

import com.fasterxml.jackson.annotation.JsonProperty





case class EdgeJson(@JsonProperty("command") val command: String,
                    @JsonProperty("srcId") val srcId: Long,
                    @JsonProperty("dstId") val dstId: Long,
                    @JsonProperty("msgTime") val msgTime: Long,
                    @JsonProperty("type") val Type: String,
                    @JsonProperty("srcLayerId") val srcLayerId: Long,
                    @JsonProperty("dstLayerId") val dstLayerId: Long,
                    @JsonProperty("properties") val properties: Map[String,Any]) {

  def isEmptyMap(): Boolean ={
    properties.isEmpty


  }


}