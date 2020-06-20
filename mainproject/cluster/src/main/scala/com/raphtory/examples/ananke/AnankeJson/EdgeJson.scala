package com.raphtory.examples.kafka.anankejson



case class EdgeJson( val command: String,
                    val srcId: Long,
                    val dstId: Long,
                    val msgTime: Long,
                    val Type: String,
                    val srcLayerId: Long,
                     val dstLayerId: Long,
                     val properties: Map[String,Any]) {

  def isEmptyMap(): Boolean ={
    properties.isEmpty


  }


}