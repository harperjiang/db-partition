package edu.uchicago.cs.dbp.hadoop.stg1.partition.rr

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.io.Source

import edu.uchicago.cs.dbp.hadoop.stg1.tool.NameMapper
object Main extends App {

  var partition = 10

  var labeled = scala.collection.mutable.HashSet[String]()

  var counter = -1
  var lastTransaction: String = null

  var output = new PrintWriter(new FileOutputStream("data/rr/partition"))

  Source.fromFile("data/rr/transaction").getLines().foreach(line => {
    var parts = line.split("\t")

    var tran = parts(0)

    if (lastTransaction == null || !tran.equals(lastTransaction)) {
      counter += 1
      counter %= partition
    }

    var dtype = parts(1)
    var dvalue = parts(2)

    var did = NameMapper.translate(dtype, dvalue)
    if (!labeled.contains(did)) {
      labeled += did
      output.println("%s\t%s\t%s".format(counter.toString(), dtype, dvalue))
    }
  })

  output.close()
}