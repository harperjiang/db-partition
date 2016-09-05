package edu.uchicago.cs.dbp.hadoop.stg1.tool.format

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import edu.uchicago.cs.dbp.hadoop.stg1.tool.NameMapper

object KeyFormatConvert extends App {

  expand("data/greedy1/partition", 0)
  expand("data/greedy1/transaction", 1)

  def expand(filename: String, column: Int) = {
    var output = new PrintWriter(new FileOutputStream("%s.tmp".format(filename)))

    Source.fromFile(filename).getLines().foreach(line => {
      var parts = line.split("\t")

      var datas = NameMapper.translate(parts(column))

      var newparts = new ArrayBuffer[String]()

      for (i <- 0 to column - 1)
        newparts += parts(i)
      newparts += datas._1
      newparts += datas._2
      for (i <- column + 1 to parts.size - 1)
        newparts += parts(i)
      output.println(newparts.mkString("\t"))
    })
    output.close()
    Runtime.getRuntime.exec("mv %s.tmp %s".format(filename, filename))
  }

  def compress(filename: String, typeColumn: Int) = {
    var output = new PrintWriter(new FileOutputStream("%s.tmp".format(filename)))

    Source.fromFile(filename).getLines().foreach(line => {
      var parts = line.split("\t")

      var did = NameMapper.translate(parts(typeColumn), parts(typeColumn + 1))

      var newparts = new ArrayBuffer[String]()

      for (i <- 0 to parts.length)
        if (typeColumn != i && typeColumn + 1 != i) {
          newparts += parts(i)
        }
      newparts.insert(typeColumn, did)
      output.println(newparts.mkString("\t"))
    })
    output.close()
    Runtime.getRuntime.exec("mv %s.tmp %s".format(filename, filename))
  }

}