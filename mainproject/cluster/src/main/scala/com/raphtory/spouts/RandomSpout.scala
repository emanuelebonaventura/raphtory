package com.raphtory.spouts

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

class RandomSpout extends SpoutTrait {

  var totalCount = 100
  var freq   = System.getenv().getOrDefault("FREQ", "1000").toInt // (Updates/s) - Hz
  val mode   = System.getenv().getOrDefault("SPOUT_MODE", "single_layer") // The other one is "multi_layer"


  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()
    println(s"Start Random Spout ($freq Hz), spout mode  = $mode ")
  }

  protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1, MILLISECONDS), "random")
    case "random" => genRandomCommands(freq / 1000)
    case _        => println("message not recognized!")
  }
  //context.system.scheduler.scheduleOnce(Duration(1, MINUTES), self, "required")
  //context.system.scheduler.schedule(Duration(90, SECONDS), Duration(30, SECONDS), self, "increase")
  //context.system.scheduler.schedule(Duration(10, SECONDS), Duration(1, MILLISECONDS), self, "random")

  def distribution(): String = {
    val random = Random.nextInt(100)
    if (mode == "multi_layer") {
      if (random >= 0 && random <= 45) genVertexAddLayer()
      else if (random >= 45 && random <= 90) genEdgeAddLayer()
      else if (random > 90 && random <= 95) genVertexRemovalLayer()
      else genEdgeRemovalLayer()
    }
    else {
      if (random >= 0 && random <= 45) genVertexAdd()
      else if (random >= 45 && random <=90) genEdgeAdd()
      else if (random > 85 && random <= 95) genVertexRemoval()
      else genEdgeRemoval()
    }
  }

  def genRandomCommands(number: Int): Unit = {
    (1 to number) foreach (_ => {
      sendTuple(distribution())
    })
    AllocateSpoutTask(Duration(1, NANOSECONDS), "random")
  }

  //NO LAYER
  def genVertexAdd(): String =
    s"""{ \"command\" : \"VertexAdd\", \"vertexId\": ${genId()},\"msgTime\": ${getTime()}, \"vertexType\": \"${genRandomString(Random.nextInt(10))}\" ${genProperties(Random.nextInt(10))}}"""

  def genEdgeAdd(): String =
    s"""{ \"command\" : \"EdgeAdd\", \"srcId\": ${genId()}, \"dstId\": ${genId()}, \"msgTime\": ${getTime()}, \"edgeType\": \"${genRandomString(Random.nextInt(10))}\" ${genProperties(Random.nextInt(10))}}"""

  def genVertexRemoval(): String =
    s"""{ \"command\" : \"VertexDelete\", \"vertexId\": ${genId()},\"msgTime\": ${getTime()}}"""

  def genEdgeRemoval(): String =
    s"""{ \"command\" : \"EdgeDelete\", \"srcId\": ${genId()}, \"dstId\": ${genId()}, \"msgTime\": ${getTime()}}"""


  //LAYER
  def genVertexAddLayer(): String =
    s"""{ \"command\" : \"VertexAdd\", \"vertexId\": ${genId()},\"msgTime\": ${getTime()}, \"vertexType\": \"${genRandomString(Random.nextInt(10))}\", \"layerId\": ${genId()} ${genProperties(Random.nextInt(10))}}"""

  def genEdgeAddLayer(): String =
    s"""{ \"command\" : \"EdgeAdd\", \"srcId\": ${genId()}, \"dstId\": ${genId()}, \"msgTime\": ${getTime()}, \"edgeType\": \"${genRandomString(Random.nextInt(10))}\", \"srcLayerId\": ${genId()},  \"dstLayerId\": ${genId()} ${genProperties(Random.nextInt(10))}}"""

  def genVertexRemovalLayer(): String =
    s"""{ \"command\" : \"VertexDelete\", \"vertexId\": ${genId()},\"msgTime\": ${getTime()}, \"layerId\": ${genId()}}"""

  def genEdgeRemovalLayer(): String =
    s"""{ \"command\" : \"EdgeDelete\", \"srcId\": ${genId()}, \"dstId\": ${genId()}, \"msgTime\": ${getTime()}, \"srcLayerId\": ${genId()}, \"dstLayerId\": ${genId()}}"""



  def genId(): Long = Random.nextLong()

  def getTime(): Long = System.currentTimeMillis()


  def genRandomString(len: Int): String = {
    val gen = Random.alphanumeric
    def build(acc: String, s: Stream[Char]): String = {
      if (s.isEmpty) acc
      else build(acc + s.head, s.tail)
    }
    build("", gen take len)
  }

  def genProperties(numOfProps: Int): String = {
    if (numOfProps == 0 ) ""
    else {
      var properties = ", \"properties\": { "
      for (i <- 1 to numOfProps) {
        if (i % 2 == 0)
          if (i == numOfProps) properties = properties + s"""\"property$i\" : ${Random.nextFloat()}"""
          else properties = properties + s""" \" property$i \" : ${Random.nextFloat()} , """
        else if (i == numOfProps) properties = properties + s"""\"property$i\" : \" ${genRandomString(Random.nextInt(10))}\" """
        else properties = properties + s"""\"property$i\" : \"${genRandomString(Random.nextInt(10))}\" , """
      }
      properties = properties + "}"
      properties
    }
  }

  def running(): Unit =
    genRandomCommands(totalCount)

}
