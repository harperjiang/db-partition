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

object ITConverterV2 extends App {

  readlink();
  
  def readlink() = {
    var conf = new Configuration();
    var job = Job.getInstance(conf, "Read Link");
    job.setJarByClass(ITConverter.getClass);
    job.setMapperClass(classOf[LinkReadV2Mapper]);
    job.setReducerClass(classOf[LinkReadV2Reducer]);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[IntArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);

    FileInputFormat.addInputPath(job, new Path("/home/harper/working/link_node"))
    FileOutputFormat.setOutputPath(job, new Path("/home/harper/working/link_link"));
    job.waitForCompletion(true)
  }

  def matchlink() = {
    var conf = new Configuration();
    var job = Job.getInstance(conf, "Match Link");
    job.setJarByClass(ITConverter.getClass);
    job.setMapperClass(classOf[LinkPairReadMapper]);
    job.setReducerClass(classOf[LinkPairReadReducer]);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[IntArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);

    FileInputFormat.addInputPath(job, new Path("/home/harper/working/link_link"))
    FileOutputFormat.setOutputPath(job, new Path("/home/harper/working/link_size_adj_weight"));
    job.waitForCompletion(true)
  }

  def renameline() = {
    var mapper = new scala.collection.mutable.HashMap[Int, Int]();
    var counter = 0;
    var output = new PrintWriter(new FileOutputStream("/home/harper/working/final"));
    Source.fromFile("/home/harper/working/link_size_adj_weight/part-r-00000").getLines().foreach {
      line =>
        {
          var values = line.split("\\s+").map(_.toInt);
          var newid = counter;
          counter += 1;

          mapper += (values(0) -> newid);
          values(0) = newid;
          for (i <- 2 until values.length by 2) {
            if (mapper.contains(values(i))) {
              values(i) = mapper.get(values(i)).get;
            }
          }
          output.println(values.mkString("\t"));
        }
    }
    output.close();
  }
}

class LinkReadV2Mapper extends Mapper[Object, Text, IntWritable, IntArrayWritable] {

  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, IntWritable, IntArrayWritable]#Context) = {
    var values = value.toString().split("\\s+");
    var link = values(0).toInt;
    var nodes = values.slice(1, values.length);

    nodes.foreach { n =>
      {
        var array = Array(link.toString, (values.length - 1).toString);
        context.write(new IntWritable(n.toInt),
          new IntArrayWritable(array))
      }
    };
  }
}

class LinkReadV2Reducer extends Reducer[IntWritable, IntArrayWritable, Text, IntWritable] {

  val threshold = 1000;

  override def reduce(key: IntWritable, values: java.lang.Iterable[IntArrayWritable],
    context: Reducer[IntWritable, IntArrayWritable, Text, IntWritable]#Context) = {

    if (values.size >= threshold) {
      // For large center, do linear edge generation
      var prev: Array[Int] = null;
      var head: Array[Int] = null;
      var intVals: Array[Int] = null;
      values.foreach { value =>
        {
          var intVals = value.get.map { _.toString.toInt };
          if (prev != null) {
            // Smaller first
            if (prev(0) < intVals(0)) {
              context.write(new Text("%d\t%d\t%d".format(prev(0), prev(1), values.size)),
                new IntWritable(intVals(0)));
            } else {
              context.write(new Text("%d\t%d\t%d".format(intVals(0), intVals(1), values.size)),
                new IntWritable(prev(0)));
            }
          } else {
            prev = intVals;
            head = prev;
          }
        }
      };
      // Finally, connect the head to tail
      if (intVals(0) < head(0)) {
              context.write(new Text("%d\t%d\t%d".format(intVals(0), intVals(1), values.size)),
                new IntWritable(head(0)));
            } else {
              context.write(new Text("%d\t%d\t%d".format(head(0), head(1), values.size)),
                new IntWritable(intVals(0)));
            }
    } else {
      // For small center, do pairwise edge generation
      var list = values.toList;
      for (i <- 0 to list.length - 1) {
        for (j <- i + 1 to list.length - 1) {
          var linki = list(i);
          var linkj = list(j);
          var idi = linki.get()(0).toString.toInt;
          var idj = linkj.get()(0).toString.toInt;
          var sizei = linki.get()(1).toString.toInt;
          var sizej = linkj.get()(1).toString.toInt;

          if (idi < idj) {
            context.write(new Text("%d\t%d\t%d".format(idi, sizei, 1)), new IntWritable(idj));
          }
          if (idi > idj) {
            context.write(new Text("%d\t%d\t%d".format(idj, sizej, 1)), new IntWritable(idi));
          }
        }
      }
    }

  }
}
