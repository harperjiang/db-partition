package edu.uchicago.cs.dbp.online.leopard.experiment.orkut

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.collection.mutable.HashSet
import org.apache.hadoop.util.hash.Hash
import edu.uchicago.cs.dbp.online.leopard.experiment.common.MetisRunner

object RunMetis extends App {
  //MetisRunner.prepareInput("leopard/orkut/edge", "leopard/orkut/metis_dict", "leopard/orkut/metis_input")
  MetisRunner.translateInput("leopard/orkut/p_metis", "leopard/orkut/metis_dict", "leopard/orkut/metis_output")
}