package edu.uchicago.cs.dbp.hadoop.stg1.tool.format

import scala.io.Source
object Average extends App {

  var sum = 0
  var count = 0
  Source.fromFile("/Users/harper/Repositories/db-partition/data/greedy1/output/tran_sum_histo/part-r-00000").getLines().foreach(line => {
    var parts = line.split("\t")

    sum += parts(0).toInt * parts(1).toInt
    count += parts(1).toInt

  })

  System.out.println(sum.toDouble / count)
}