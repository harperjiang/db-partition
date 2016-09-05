package edu.uchicago.cs.dbp.leopard.eval

import java.io.PrintWriter
import edu.uchicago.cs.dbp.model.Partition

object PartitionPrinter {

  def print(ps: Iterable[Partition], out: PrintWriter): Unit = {
    ps.foreach(p => {
      p.vertices.foreach(v => out.println("%d\t%d".format(p.id, v.id)))
    });
  }
}