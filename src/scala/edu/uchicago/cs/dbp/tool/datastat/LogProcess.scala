package edu.uchicago.cs.dbp.tool.datastat

import java.util.regex.Pattern
import scala.collection.mutable.HashSet
import scala.io.Source

object LogProcss extends App {
  var ptnEt = Pattern.compile("END;\\d+")
  var ptnTc = Pattern.compile("(\\d+);([a-z]+),(\\d+)")

  var folder = "/home/harper/Downloads/txnLog/affinity-5GB-hotsupplier-lowskew/monitor-%d/transactions-partition-%d.log"

  var logProcessor = new CountNode()

  for (j <- 1 to 9) {
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