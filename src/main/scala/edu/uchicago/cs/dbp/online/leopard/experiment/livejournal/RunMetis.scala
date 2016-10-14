package edu.uchicago.cs.dbp.online.leopard.experiment.livejournal

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.collection.mutable.HashSet
import edu.uchicago.cs.dbp.online.leopard.experiment.common.MetisRunner

object RunMetis extends App {
  //MetisRunner.prepareInput("leopard/livejournal/edge", "leopard/livejournal/metis_dict", "leopard/livejournal/metis_input")
  MetisRunner.translateInput("leopard/livejournal/p_metis", "leopard/livejournal/metis_dict", "leopard/livejournal/metis_output")
}