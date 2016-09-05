package edu.uchicago.cs.dbp.hadoop.stg1.tool.format

import java.io.PrintWriter
import java.io.FileOutputStream
import edu.uchicago.cs.dbp.hadoop.stg1.tool.NameMapper
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
object ReColumn extends App {

  recolumn("data/rr/partition", Array(0, 1))

  def recolumn(filename: String, newseq: Array[Int]) = {
    var output = new PrintWriter(new FileOutputStream("%s.tmp".format(filename)))

    Source.fromFile(filename).getLines().foreach(line => {
      var parts = line.split("\t")

      var newparts = new ArrayBuffer[String]()

      for (i <- 0 to newseq.length - 1)
        newparts += parts(newseq(i))
      output.println(newparts.mkString("\t"))
    })
    output.close()
    // Runtime.getRuntime.exec("mv %s.tmp %s".format(filename, filename))
  }
}