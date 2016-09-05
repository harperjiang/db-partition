package edu.uchicago.cs.dbp.hadoop.stg1.tool.datastat

import java.util.regex.Pattern
import scala.io.Source
import java.io.FileOutputStream
import java.io.PrintWriter

object LogProcess extends App {
  var ptnEt = Pattern.compile("END;(\\d+)")
  var ptnTc = Pattern.compile("(\\d+);([a-z]+),(\\d+)")

  var folder = "/home/harper/Downloads/txnLog/affinity-5GB-hotproduct/monitor-%d/transactions-partition-%d.log"

  var logProcessor = new FlattenTransaction(new PrintWriter(new FileOutputStream("data/greedy1/transaction")))

  for (j <- 0 to 1) {
    for (i <- 0 to 29) {
      var fileName = folder.format(j, i)
      Source.fromFile(fileName).getLines().foreach { line =>
        {
          var etmatcher = ptnEt.matcher(line)
          if (etmatcher.matches()) {
            logProcessor.endTran(etmatcher.group(1))
          } else {
            var matcher = ptnTc.matcher(line)
            if (matcher.matches()) {
              logProcessor.addLine(matcher.group(1), matcher.group(2), matcher.group(3))
            }
          }
        }
      }
    }
  }
  logProcessor.endProcess()
}