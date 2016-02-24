package edu.uchicago.cs.dbp.tool.datastat

import scala.io.Source
import java.util.regex.Pattern
import scala.collection.mutable.HashSet

object CountNode extends App {

  var ptnEt = Pattern.compile("END;\\d+")
  var ptnTc = Pattern.compile("(\\d+);([a-z]+),(\\d+)")

  var datastore = scala.collection.mutable.HashMap[String, HashSet[Int]]()

  var folder = "/home/harper/Downloads/txnLog/affinity-5GB-hotproduct/monitor-0"
  for (i <- 0 to 29) {
    var fileName = "{0}/transactions-partition-{1}.log".format(folder, i)
    Source.fromFile(fileName).getLines().foreach { line =>
      {
        if (ptnEt.matcher(line).matches()) {
          // Ignore the line
        } else {
          var matcher = ptnTc.matcher(line)
          if (matcher.matches()) {
            var datatype = matcher.group(2)
            var dataval = matcher.group(3).toInt

            datastore.getOrElseUpdate(datatype, new HashSet[Int]).add(dataval)
          }
        }
      }
    }
  }
  datastore.foreach(f => { System.out.println(f._1 + ":" + f._2.size) })
}