package com.raphtory.core.model.graphentities

import scala.collection.parallel.mutable.ParTrieMap

object Layer {
  def apply(
             creationTime: Long,
             layerID: Long,
           ) = {
    val l = new Layer(creationTime, layerID)
    l
  }

}


class Layer(creationTime: Long, val layerID: Long) {

  val vertices = ParTrieMap[Long, Vertex]()

  override def toString: String =
    s"Layer layerID $layerID creationTime $creationTime \n Vertex $vertices "

}


