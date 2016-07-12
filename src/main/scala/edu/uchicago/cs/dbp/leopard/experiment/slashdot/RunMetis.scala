package edu.uchicago.cs.dbp.leopard.experiment.slashdot

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
  // MetisRunner.prepareInput("leopard/slashdot/edge", "leopard/slashdot/metis_dict", "leopard/slashdot/metis_input")
  MetisRunner.translateInput("leopard/slashdot/p_metis", "leopard/slashdot/metis_dict", "leopard/slashdot/metis_output")
}