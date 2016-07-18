package edu.uchicago.cs.dbp.leopard.experiment.stanford

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
  //MetisRunner.prepareInput("leopard/stanford/edge", "leopard/stanford/metis_dict", "leopard/stanford/metis_input")
  MetisRunner.translateInput("leopard/stanford/p_metis", "leopard/stanford/metis_dict", "leopard/stanford/metis_output")
}