package edu.uchicago.cs.dbp.hadoop.stg1.eval

import scala.io.Source
import java.io.File

object FileCompare {

  def compare(input1: String, input2: String): Boolean = {

    if (Source.fromFile(input1).size != Source.fromFile(input2).size) {
      System.err.println("Different size");
      return false;
    }

    var lines1 = Source.fromFile(input1).getLines();
    var lines2 = Source.fromFile(input2).getLines();

    lines1.foreach { line1 =>
      {
        var line2 = lines2.next();

        if (line1 != line2) {
          System.err.println("%s<==>%s".format(line1, line2));
          return false;
        }
      }
    };

    return true;
  }

  def compareContent(input1: String, input2: String): Boolean = {

    var lines1: Iterator[String] = null;
    var lines2: Iterator[String] = null;
    if (new File(input1).isFile()) {
      lines1 = Source.fromFile(input1).getLines();
    } else {
      lines1 = new File(input1).listFiles().filter(_.isFile).filter { f => !(f.getName.equals("_SUCCESS") || f.getName.startsWith(".")) }
        .toIterator.flatMap(Source fromFile _ getLines)
    }
    if (new File(input2).isFile()) {
      lines2 = Source.fromFile(input2).getLines();
    } else {
      lines2 = new File(input2).listFiles().filter(_.isFile)
        .filter { f => !(f.getName.equals("_SUCCESS") || f.getName.startsWith(".")) }.toIterator.flatMap(Source fromFile _ getLines)
    }
    var data1 = Map[String, Set[String]]();

    lines1.foreach { line1 =>
      {
        var parts = line1.split("\\s+");
        if (!data1.contains(parts(0))) {
          data1 += { parts(0) -> Set(parts(1)) };
        }
        data1 += { parts(0) -> { data1.get(parts(0)).get + parts(1) } }
      }
    }
    lines2.foreach { line2 =>
      {
        var parts = line2.split("\\s+")
        if (!data1.contains(parts(0)) || !data1.get(parts(0)).get.contains(parts(1))) {
          System.err.println(line2)
          return false;
        }
      }
    }
    return true;
  }
}