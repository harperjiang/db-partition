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

object ITConverter extends App {

  def parse() = {
    var parser = new Parser();
    var output = new PrintWriter(new FileOutputStream(""));
    Source.fromFile("").getLines().foreach { line =>
      {
        if (!line.startsWith("#")) {
          var link = parser.parse[Link](line);
          output.println(link.id + " " + link.nodes.map(_.id).mkString(" "));
        }
      }
    };
    output.close();
  }

  def readlink() = {
    var conf = new Configuration();
    var job = Job.getInstance(conf, "Read Link");
    job.setJarByClass(ITConverter.getClass);
    job.setMapperClass(classOf[LinkReadMapper]);
    job.setReducerClass(classOf[LinkReadReducer]);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[IntArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);

    FileInputFormat.addInputPath(job, new Path("/home/harper/working/link_node"))
    FileOutputFormat.setOutputPath(job, new Path("/home/harper/working/node_link"));
    job.waitForCompletion(true)
  }
}

class LinkReadMapper extends Mapper[Object, Text, IntWritable, IntArrayWritable] {

  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, IntWritable, IntArrayWritable]#Context) = {
    var values = value.toString().split("\\s");
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

class LinkReadReducer extends Reducer[IntWritable, IntArrayWritable, Text, IntWritable] {

  override def reduce(key: IntWritable, values: java.lang.Iterable[IntArrayWritable],
    context: Reducer[IntWritable, IntArrayWritable, Text, IntWritable]#Context) = {
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
          context.write(new Text("%d\t%d".format(idi, sizei)), new IntWritable(idj));
        }
        if (idi > idj) {
          context.write(new Text("%d\t%d".format(idj, sizej)), new IntWritable(idi));
        }
      }
    }
  }
}

class LinkPairReadMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    var values = value.toString().split("\\s");
    context.write(new IntWritable(values(0).toInt), new IntWritable(values(1).toInt));
  }
}

class LinkPairReadReducer extends Reducer[IntWritable, IntWritable, IntWritable, Text] {

  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, Text]#Context) = {
    var from = key;

  }
}