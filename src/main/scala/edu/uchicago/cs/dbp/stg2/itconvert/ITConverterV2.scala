package edu.uchicago.cs.dbp.stg2.itconvert

import java.io.FileOutputStream
import java.io.PrintWriter

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.parser.Parser
import edu.uchicago.cs.dbp.common.types.IntArrayWritable
import edu.uchicago.cs.dbp.common.types.KeyGroupComparator.Key2GroupComparator
import edu.uchicago.cs.dbp.common.types.KeyPartitioner.Key2Partitioner
import scala.collection.mutable.ArrayBuffer

object ITConverterV2 extends App {

  countedge2();

  def countedge() = {
    var mapper = new scala.collection.mutable.HashSet[(Int, Int)]();
    Source.fromFile("/home/harper/storage/workingbig/v2/final").getLines().foreach {
      line =>
        {
          var values = line.split("\\s+").map(_.toInt);
          for (i <- 2 until values.length by 2) {
            var a = values(0);
            var b = values(i);
            if (a <= b) {
              mapper += ((a, b));
            } else {
              mapper += ((b, a));
            }
          }
        }
    }

    System.out.println(mapper.size);
  }

  def countedge2() = {
    var mapper = new scala.collection.mutable.HashSet[(Int, Int)]();
    var counter = 1;
    Source.fromFile("/home/harper/storage/workingbig/v2/final").getLines().foreach {
      line =>
        {
          var values = line.split("\\s+").map(_.toInt);
          for (i <- 1 until values.length by 2) {
            var a = counter;
            var b = values(i);
            if (a <= b) {
              mapper += ((a, b));
            } else {
              mapper += ((b, a));
            }
          }
          counter+=1;
        }
    }

    System.out.println(mapper.size);
  }

}
