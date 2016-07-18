package edu.uchicago.cs.dbp.leopard.experiment.berkstan

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
  //MetisRunner.prepareInput("leopard/berkstan/edge", "leopard/berkstan/metis_dict", "leopard/berkstan/metis_input")
  MetisRunner.translateInput("leopard/berkstan/p_metis", "leopard/berkstan/metis_dict", "leopard/berkstan/metis_output")
}