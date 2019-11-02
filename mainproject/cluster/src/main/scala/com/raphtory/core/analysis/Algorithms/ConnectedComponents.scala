package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.communication.VertexMessage
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
case class ClusterLabel(value: Int) extends VertexMessage
class ConnectedComponents extends Analyser {


  override def setup() = {
    proxy.getVerticesSet().foreach(v => {
      var min = v._1
      val vertex = proxy.getVertex(v._2)
      //math.min(v, vertex.getOutgoingNeighbors.union(vertex.getIngoingNeighbors).min)
      val toSend = vertex.getOrSetCompValue("cclabel", min).asInstanceOf[Int]
      vertex.messageAllNeighbours(ClusterLabel(toSend))
    })
  }

  override def analyse(): Any = {

    proxy.getVerticesSet().map(vert=>{
      var label = vert._1
      val vertex = proxy.getVertex(vert._2)
      val queue = vertex.messageQueue
      if (queue.nonEmpty) {
        label = queue.map(_.asInstanceOf[ClusterLabel].value).min
        //vertex.messageQueue.clear()
      }
      var currentLabel = vertex.getOrSetCompValue("cclabel", label).asInstanceOf[Int]
      if (label < currentLabel) {
        vertex.setCompValue("cclabel", label)
        vertex messageAllOutgoingNeighbors (ClusterLabel(label))
        vertex messageAllIngoingNeighbors ClusterLabel(label)
        currentLabel = label
      }
      //else {
      //vertex messageAllNeighbours (ClusterLabel(currentLabel))
      //  vertex.voteToHalt()
      //}
      currentLabel
    }).groupBy(f=> f).map(f=> (f._1,f._2.size))
  }

  override def processResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any],viewCompleteTime:Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Int, Int]]]
    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
    val startTime = System.currentTimeMillis()
    try {
      val grouped = endResults.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum)
      val groupedNonIslands = grouped.filter(x=>x._2>1)
      val biggest = grouped.maxBy(_._2)._2
      val total = grouped.size
      val totalWithoutIslands = groupedNonIslands.size
      val totalIslands = total-totalWithoutIslands
      val proportion = biggest.toFloat/grouped.map(x=> x._2).sum
      val proportionWithoutIslands = biggest.toFloat/groupedNonIslands.map(x=> x._2).sum
      val totalGT2 = grouped.filter(x=> x._2>2).size
      val text = s"""{"time":${EntityStorage.newestTime},"biggest":$biggest,"total":$total,"totalWithoutIslands":$totalWithoutIslands,"totalIslands":$totalIslands,"proportion":$proportion,"proportionWithoutIslands":$proportionWithoutIslands,"clustersGT2":$totalGT2,"viewTime":$viewCompleteTime,"concatTime":${System.currentTimeMillis()-startTime}},"""
      Utils.writeLines(output_file,text,"{\"views\":[")
      println(text)
    }catch {
      case e:UnsupportedOperationException => println(s"No activity for  view at ${EntityStorage.newestTime}")
    }
  }

  override def processViewResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long,viewCompleteTime:Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Int, Int]]]
    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
    val startTime = System.currentTimeMillis()
    try {
      val grouped = endResults.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum)
      val groupedNonIslands = grouped.filter(x=>x._2>1)
      val biggest = grouped.maxBy(_._2)._2
      val total = grouped.size
      val totalWithoutIslands = groupedNonIslands.size
      val totalIslands = total-totalWithoutIslands
      val proportion = biggest.toFloat/grouped.map(x=> x._2).sum
      val proportionWithoutIslands = biggest.toFloat/groupedNonIslands.map(x=> x._2).sum
      val totalGT2 = grouped.filter(x=> x._2>2).size
      val text = s"""{"time":$timestamp,"biggest":$biggest,"total":$total,"totalWithoutIslands":$totalWithoutIslands,"totalIslands":$totalIslands,"proportion":$proportion,"proportionWithoutIslands":$proportionWithoutIslands,"clustersGT2":$totalGT2,"viewTime":$viewCompleteTime,"concatTime":${System.currentTimeMillis()-startTime}},"""
      Utils.writeLines(output_file,text,"{\"views\":[")
      println(text)
    }catch {
      case e:UnsupportedOperationException => println(s"No activity for  view at $timestamp")
    }
  }

  override def processWindowResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any],timestamp:Long,windowSize:Long,viewCompleteTime:Long): Unit = {

    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Int, Int]]]
    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
    val startTime = System.currentTimeMillis()
    try {
      val grouped = endResults.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum)
      val groupedNonIslands = grouped.filter(x=>x._2>1)
      val biggest = grouped.maxBy(_._2)._2
      val total = grouped.size
      val totalWithoutIslands = groupedNonIslands.size
      val totalIslands = total-totalWithoutIslands
      val proportion = biggest.toFloat/grouped.map(x=> x._2).sum
      val proportionWithoutIslands = biggest.toFloat/groupedNonIslands.map(x=> x._2).sum
      val totalGT2 = grouped.filter(x=> x._2>2).size
      val text = s"""{"time":$timestamp,"windowsize":$windowSize,"biggest":$biggest,"total":$total,"totalWithoutIslands":$totalWithoutIslands,"totalIslands":$totalIslands,"proportion":$proportion,"proportionWithoutIslands":$proportionWithoutIslands,"clustersGT2":$totalGT2,"viewTime":$viewCompleteTime,"concatTime":${System.currentTimeMillis()-startTime}},"""
      Utils.writeLines(output_file,text,"{\"views\":[")
      println(text)
    }catch {
      case e:UnsupportedOperationException => println(s"No activity for  view at $timestamp with window $windowSize")
    }


  }

  override def processBatchWindowResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long, windowSet: Array[Long],viewCompleteTime:Long): Unit = {
    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
    val endResults = results.asInstanceOf[ArrayBuffer[ArrayBuffer[immutable.ParHashMap[Int, Int]]]]
    for(i <- endResults.indices){
      val startTime = System.currentTimeMillis()
      val window = endResults(i)
      val windowSize = windowSet(i)
      try {
        val grouped = window.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum)
        val groupedNonIslands = grouped.filter(x=>x._2>1)
        val biggest = grouped.maxBy(_._2)._2
        val total = grouped.size
        val totalWithoutIslands = groupedNonIslands.size
        val totalIslands = total-totalWithoutIslands
        val proportion = biggest.toFloat/grouped.map(x=> x._2).sum
        val proportionWithoutIslands = biggest.toFloat/groupedNonIslands.map(x=> x._2).sum
        val totalGT2 = grouped.filter(x=> x._2>2).size
        //println(s"$timestamp $windowSize $biggest $total $window")
        val text = s"""{"time":$timestamp,"windowsize":$windowSize,"biggest":$biggest,"total":$total,"totalWithoutIslands":$totalWithoutIslands,"totalIslands":$totalIslands,"proportion":$proportion,"proportionWithoutIslands":$proportionWithoutIslands,"clustersGT2":$totalGT2,"viewTime":$viewCompleteTime,"concatTime":${System.currentTimeMillis()-startTime}},"""
        Utils.writeLines(output_file,text,"{\"views\":[")
        println(text)
        //println(s"At ${new Date(timestamp)} with a window of ${windowSize / 3600000} hour(s) there were ${} connected components. The biggest being ${}")
      }catch {
        case e:UnsupportedOperationException => println(s"No activity for  view at $timestamp with window $windowSize")
      }
    }

  }

  override def defineMaxSteps(): Int = 10
  override def checkProcessEnd(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any]) : Boolean = {false
    // move to LAM partitionsHalting()
  }


  //
  //override def analyse(): Any = {
  //  val results = ParTrieMap[Int, Int]()
  //  var verts = Set[Int]()
  //  for (v <- proxy.getVerticesSet()) {
  //    val vertex = proxy.getVertex(v)
  //    val queue = vertex.messageQueue.map(_.asInstanceOf[ClusterLabel].value)
  //    var label = v
  //    if (queue.nonEmpty)
  //      label = queue.min
  //    vertex.messageQueue.clear
  //    var currentLabel = vertex.getOrSetCompValue("cclabel", v).asInstanceOf[Int]
  //    if (label < currentLabel) {
  //      vertex.setCompValue("cclabel", label)
  //      vertex messageAllNeighbours (ClusterLabel(label))
  //      currentLabel = label
  //    }
  //    else {
  //      vertex messageAllNeighbours (ClusterLabel(currentLabel))
  //      vertex.voteToHalt()
  //    }
  //    results.put(currentLabel, 1 + results.getOrElse(currentLabel, 0))
  //    verts += v
  //  }
  //  results
  //}
  //  override def analyse(): Any = {
  //    val x = proxy.asInstanceOf[WindowProxy].keySet.foreach(vertex => {
  //
  //      val queue = vertex._2.messageQueue.map(_.asInstanceOf[ClusterLabel].value)
  //      var label = vertex._1
  //      if (queue.nonEmpty)
  //        label = queue.min
  //      vertex._2.messageQueue.clear
  //      var currentLabel = vertex._2.getOrSetCompValue("cclabel", label).asInstanceOf[Int]
  //      if (label < currentLabel) {
  //        vertex._2.setCompValue("cclabel", label)
  //        vertex._2 messageAllNeighbours (ClusterLabel(label))
  //        currentLabel = label
  //      }
  //      else {
  //        vertex._2.messageAllNeighbours (ClusterLabel(currentLabel))
  //        //vertex.voteToHalt()
  //      }
  //      currentLabel
  //    })
  //  }
}
