package edu.uchicago.cs.dbp.leopard.experiment.notredame

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.collection.mutable.HashSet
import org.apache.hadoop.util.hash.Hash
import edu.uchicago.cs.dbp.leopard.experiment.common.MetisRunner

object RunMetis extends App {
  //MetisRunner.prepareInput("leopard/notredame/edge", "leopard/notredame/metis_dict", "leopard/notredame/metis_input")
  MetisRunner.translateInput("leopard/notredame/p_metis", "leopard/notredame/metis_dict", "leopard/notredame/metis_output")
}