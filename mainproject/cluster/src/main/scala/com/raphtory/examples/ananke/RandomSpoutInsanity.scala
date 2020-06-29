package com.raphtory.examples.ananke

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.utils.Utils

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

class RandomSpoutInsanity extends SpoutTrait {

  var totalCount = 100
  var freq   = System.getenv().getOrDefault("FREQ", "1000").toInt // (Updates/s) - Hz
  var pool       = System.getenv().getOrDefault("ENTITY_POOL", "1000000").toInt
  var msgID      = 0

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()
    println(s"Start Random Spout ($freq Hz) IDs pool = $pool ")

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
    if (random >= 0 && random <= 25) genVertexAdd()
    else if (random > 25 && random <= 50) genEdgeAdd()
    else if (random > 50 && random <= 75) genVertexRemoval()
    else genEdgeRemoval()

  }

  def genRandomCommands(number: Int): Unit = {
    (1 to number) foreach (_ => {
      sendTuple(distribution())
    })
    AllocateSpoutTask(Duration(1, NANOSECONDS), "random")
  }

  def genVertexAdd(): String =
    s""" {"VertexAdd":{${getMessageID()}, ${genSrcID()}, ${genProperties(2)}}}"""
  def genVertexAdd(src: Int): String = //overloaded method if you want to specify src ID
    s""" {"VertexAdd":{${getMessageID()}, ${genSrcID(src)}, ${genProperties(2)}}}"""


  def genVertexRemoval(): String =
    s""" {"VertexRemoval":{${getMessageID()}, ${genSrcID()}}}"""

  def genVertexRemoval(src: Int): String =
    s""" {"VertexRemoval":{${getMessageID()}, ${genSrcID(src)}}}"""

  def genEdgeAdd(): String =
    s""" {"EdgeAdd":{${getMessageID()}, ${genSrcID()}, ${genDstID()}, ${genProperties(2)}}}"""
  def genEdgeAdd(src: Int, dst: Int): String =
    s""" {"EdgeAdd":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}}}"""

  def genEdgeRemoval(): String =
    s""" {"EdgeRemoval":{${getMessageID()}, ${genSrcID()}, ${genDstID()}}}"""
  def genEdgeRemoval(src: Int, dst: Int): String =
    s""" {"EdgeRemoval":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}}}"""



  def genSrcID(): String = s""" "srcID":${Random.nextInt(pool)} """
  def genDstID(): String = s""" "dstID":${Random.nextInt(pool)} """

  def genSrcID(src: Int): String = s""" "srcID":$src """
  def genDstID(dst: Int): String = s""" "dstID":$dst """

  def getMessageID(): String = {
    msgID += 1
    //s""" "messageID":${System.currentTimeMillis()} """
    s""" "messageID":$msgID """
  }

  def genProperties(numOfProps: Int): String = {
    var properties = "\"properties\":{"
    for (i <- 1 to numOfProps) {
      val propnum = i
      if (i < numOfProps) properties = properties + s""" "property$propnum":"${"test"}", """
      else properties = properties + s""" "property$propnum":"${"test"}" }"""
    }
    properties
  }

  def running(): Unit =
    genRandomCommands(totalCount)
  //totalCount+=1000

}
