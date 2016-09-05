package edu.uchicago.cs.dbp.hadoop.stg1.eval

import org.apache.hadoop.fs.Path
object BaseFolder {
  var basefolder = "data/greedy1"

  def path(sub: String): Path = {
    return new Path("%s/%s".format(basefolder, sub))
  }
}