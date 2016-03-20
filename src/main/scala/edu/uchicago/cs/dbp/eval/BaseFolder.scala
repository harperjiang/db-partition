package edu.uchicago.cs.dbp.eval

import org.apache.hadoop.fs.Path
object BaseFolder {
  var basefolder = "data/rr"

  def path(sub: String): Path = {
    return new Path("%s/%s".format(basefolder, sub))
  }
}