package edu.uchicago.cs.dbp.online.leopard.experiment.google

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.collection.mutable.HashSet
import edu.uchicago.cs.dbp.online.leopard.experiment.common.MetisRunner

object RunMetis extends App {
  //MetisRunner.prepareInput("leopard/google/edge", "leopard/google/metis_dict", "leopard/google/metis_input")
  MetisRunner.translateInput("leopard/google/p_metis", "leopard/google/metis_dict", "leopard/google/metis_output")
}