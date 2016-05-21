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

  mergelink2();

  def parse() = {
    var parser = new Parser();
    var output = new PrintWriter(new FileOutputStream("/home/harper/working/link_node"));
    Source.fromFile("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/kapar-midar-iff.links").getLines().foreach { line =>
      {
        if (!line.startsWith("#")) {
          var link = parser.parse[Link](line);
          output.println(link.id + " " + link.anonymousNodeIds.mkString(" ") + " " + link.namedNodeIds.values.mkString(" "));
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
    FileOutputFormat.setOutputPath(job, new Path("/home/harper/working/link_link_size"));
    job.waitForCompletion(true)
  }

  def matchlink() = {
    var conf = new Configuration();
    var job = Job.getInstance(conf, "Match Link");
    job.setJarByClass(ITConverter.getClass);
    job.setMapperClass(classOf[LinkPairReadMapper]);
    job.setReducerClass(classOf[LinkPairReadReducer]);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(classOf[IntArrayWritable]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);

    FileInputFormat.addInputPath(job, new Path("/home/harper/storage/workingbig/link_link"))
    FileOutputFormat.setOutputPath(job, new Path("/home/harper/storage/workingbig/edge_weight"));
    job.waitForCompletion(true)
  }

  def mergelink() = {
    var conf = new Configuration();
    var job = Job.getInstance(conf, "Merge Link");
    job.setJarByClass(ITConverter.getClass);
    job.setMapperClass(classOf[MergeMapper]);
    job.setReducerClass(classOf[MergeReducer]);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(classOf[IntArrayWritable]);
    job.setMapOutputValueClass(classOf[IntArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);

    FileInputFormat.addInputPath(job, new Path("/home/harper/storage/workingbig/edge_weight"))
    FileOutputFormat.setOutputPath(job, new Path("/home/harper/storage/workingbig/edge_merge"));
    job.waitForCompletion(true)
  }

  def mergelink2() = {
    var inputfile = "/home/harper/storage/workingbig/edge_merge/part-r-00000";
    var previd: Int = -1;
    var outputfile = "/home/harper/storage/workingbig/merged";
    var pw = new PrintWriter(new FileOutputStream(outputfile));
    Source.fromFile(inputfile).getLines().foreach {
      line =>
        {
          var split = line.split("\\s+").map(_.toInt);
          var curid = split(0);
          if (previd == -1) {
            previd = curid;
            pw.print("%d\t%d\t".format(split(0), split(1)));
          } else if (previd != curid) {
            previd = curid;
            pw.println();
            pw.print("%d\t%d\t".format(split(0), split(1)));
          } else {
            pw.print("%d\t%d\t".format(split(2), split(3)));
          }
        }
    }
    pw.close();
  }

  def renameline() = {
    var inputFile = "/home/harper/storage/workingbig/edge_weight/part-r-00000";
    var mapper = new scala.collection.mutable.HashMap[Int, Int]();
    var counter = 0;
    var output = new PrintWriter(new FileOutputStream("/home/harper/storage/workingbig/final"));
    var mappingfile = new PrintWriter(new FileOutputStream("/home/harper/storage/workingbig/mapping"));
    // Two pass, the first pass build mapping table, the second pass rewrite line
    // Input : src_id, src_weight, [dest_id, edge_weight]+
    // Output: src_weight(src_id is the num of line), [dest_id, edge_weight]+
    Source.fromFile(inputFile).getLines().foreach {
      line =>
        {
          var parts = line.split("\\s+");
          counter = counter + 1;
          mapper += (parts(0).toInt -> counter);
          mappingfile.println("%d\t%d".format(counter, parts(0).toInt));
        }
    };
    mappingfile.close();

    var sbf = new StringBuilder();
    Source.fromFile(inputFile).getLines().foreach {
      line =>
        {
          var values = line.split("\\s+").map(_.toInt);
          sbf.clear();
          // Node weight
          sbf.append(values(1)).append("\t");
          for (i <- 2 until values.length by 2) {
            sbf.append(
              mapper.getOrElse(values(i),
                { throw new RuntimeException("Unrecognized node:%d".format(values(i))); })).append("\t");
            sbf.append(values(i + 1)).append("\t");
          }
          output.println(sbf.toString());
        }
    }
    output.close();
  }
}

class LinkReadMapper extends Mapper[Object, Text, IntWritable, IntArrayWritable] {

  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, IntWritable, IntArrayWritable]#Context) = {
    var values = value.toString().split("\\s+");
    var link = values(0).toInt;
    var nodes = values.slice(1, values.length);
    var array = Array(link, (values.length - 1));
    nodes.foreach { n =>
      {
        context.write(new IntWritable(n.toInt),
          new IntArrayWritable(array))
      }
    };
  }
}

class LinkReadReducer extends Reducer[IntWritable, IntArrayWritable, Text, IntWritable] {

  override def reduce(key: IntWritable, values: java.lang.Iterable[IntArrayWritable],
    context: Reducer[IntWritable, IntArrayWritable, Text, IntWritable]#Context) = {

    //context.write(new Text(key.get.toString), new IntWritable(values.size));

    var list = new scala.collection.mutable.ArrayBuffer[Array[Int]]();
    values.foreach(value => { list += value.get.map { _.toString.toInt } });
    for (i <- 0 to list.length - 1) {
      for (j <- i + 1 to list.length - 1) {
        var linki = list(i);
        var linkj = list(j);
        var idi = linki(0).toInt;
        var idj = linkj(0).toInt;
        var sizei = linki(1).toInt;
        var sizej = linkj(1).toInt;

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

class LinkPairReadMapper extends Mapper[Object, Text, IntArrayWritable, IntWritable] {

  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, IntArrayWritable, IntWritable]#Context) = {
    var values = value.toString().split("\\s+");
    // Input: src src_size weight dest dest_size 
    context.write(new IntArrayWritable(Array(values(0), values(1), values(3), values(4))), new IntWritable(values(2).toInt));
  }
}

class LinkPairReadReducer extends Reducer[IntArrayWritable, IntWritable, Text, Text] {

  override def reduce(key: IntArrayWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntArrayWritable, IntWritable, Text, Text]#Context) = {

    var sum = 0
    values.foreach { sum += _.get };

    context.write(new Text(key.get.map { _.toString }.mkString("\t")),
      new Text("%d".format(sum)));
  }
}

class MergeMapper extends Mapper[Object, Text, IntArrayWritable, IntArrayWritable] {
  // Input: src src_size weight dest dest_size 
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntArrayWritable, IntArrayWritable]#Context) = {
    var values = value.toString.split("\\s+").map(_.toInt);
    context.write(new IntArrayWritable(Array(values(0), values(1))), new IntArrayWritable(Array(values(3), values(2))));
    context.write(new IntArrayWritable(Array(values(3), values(4))), new IntArrayWritable(Array(values(0), values(2))));
  }
}

class MergeReducer extends Reducer[IntArrayWritable, IntArrayWritable, Text, Text] {
  // Input : key : src, src weight,  values: dest, edge weight
  override def reduce(key: IntArrayWritable, values: java.lang.Iterable[IntArrayWritable],
    context: Reducer[IntArrayWritable, IntArrayWritable, Text, Text]#Context) = {

    values.foreach(value => {
      // dest_id, edge_weight
      var intval = value.get.map(_.toString.toInt);
      context.write(new Text(key.get.map(_.toString).mkString("\t")), new Text(intval.mkString("\t")));
    });
  }
}