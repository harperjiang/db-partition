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

object ITConverter extends App {

  val basefolder = "/home/harper/storage/workingbig/v2/"

  renameline();

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
    job.setMapperClass(classOf[LinkReadV2Mapper]);
    job.setReducerClass(classOf[LinkReadV2Reducer]);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(classOf[IntWritable]);
    job.setMapOutputValueClass(classOf[IntArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);

    FileInputFormat.addInputPath(job, new Path("/home/harper/working/link_node"))
    FileOutputFormat.setOutputPath(job, new Path(basefolder + "link_link"));
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

    FileInputFormat.addInputPath(job, new Path(basefolder + "link_link"))
    FileOutputFormat.setOutputPath(job, new Path(basefolder + "edge_weight"));
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

    FileInputFormat.addInputPath(job, new Path(basefolder + "edge_weight"))
    FileOutputFormat.setOutputPath(job, new Path(basefolder + "edge_merge"));
    job.waitForCompletion(true)
  }

  def renameline() = {
    var inputFile = basefolder + "edge_merge/part-r-00000";
    var mapper = new scala.collection.mutable.HashMap[Int, Int]();
    var counter = 0;
    var output = new PrintWriter(new FileOutputStream(basefolder + "final"));
    var mappingfile = new PrintWriter(new FileOutputStream(basefolder + "mapping"));
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

  def countedge() = {
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
        }
    }

    // 56020598
    System.out.println(mapper.size);
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

class LinkReadV2Reducer extends Reducer[IntWritable, IntArrayWritable, Text, Text] {

  val threshold = 1;

  override def reduce(key: IntWritable, values: java.lang.Iterable[IntArrayWritable],
    context: Reducer[IntWritable, IntArrayWritable, Text, Text]#Context) = {

    var list = new ArrayBuffer[Array[Int]]();

    values.foreach(value => { list += value.get.map({ _.toString.toInt }) });

    //context.write(new Text("%d".format(key.get)),new Text("%d".format(list.size)));
    //context.write(new Text("%d".format(key.get)),new Text(list.map(_.mkString(" ")).mkString("\t")));
    if (list.size >= threshold) {
      // For large center, do linear edge generation
      var prev: Array[Int] = null;
      var head: Array[Int] = null;
      var intVals: Array[Int] = null;
      list.foreach { value =>
        {
          intVals = value;
          if (prev != null) {
            // Smaller first
            if (prev(0) < intVals(0)) {
              context.write(new Text("%d\t%d\t%d".format(prev(0), prev(1), list.size)),
                new Text("%d\t%d".format(intVals(0), intVals(1))));
            } else {
              context.write(new Text("%d\t%d\t%d".format(intVals(0), intVals(1), list.size)),
                new Text("%d\t%d".format(prev(0), prev(1))));
            }
          }
          prev = intVals;
          if (null == head)
            head = intVals;
        }
      };
      // Finally, connect the head to tail
      if (intVals(0) < head(0)) {
        context.write(new Text("%d\t%d\t%d".format(intVals(0), intVals(1), list.size)),
          new Text("%d\t%d".format(head(0), head(1))));
      } else {
        context.write(new Text("%d\t%d\t%d".format(head(0), head(1), list.size)),
          new Text("%d\t%d".format(intVals(0), intVals(1))));
      }
    } else {
      // For small center, do pairwise edge generation
      for (i <- 0 to list.length - 1) {
        for (j <- i + 1 to list.length - 1) {
          var linki = list(i);
          var linkj = list(j);
          var idi = linki(0);
          var idj = linkj(0);
          var sizei = linki(1);
          var sizej = linkj(1);

          if (idi < idj) {
            context.write(new Text("%d\t%d\t%d".format(idi, sizei, 1)), new Text("%d\t%d".format(idj, sizej)));
          }
          if (idi > idj) {
            context.write(new Text("%d\t%d\t%d".format(idj, sizej, 1)), new Text("%d\t%d".format(idi, sizei)));
          }
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
  // Input: src src_size dest dest_size weight
  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntArrayWritable, IntArrayWritable]#Context) = {
    var values = value.toString.split("\\s+").map(_.toInt);
    context.write(new IntArrayWritable(Array(values(0), values(1))), new IntArrayWritable(Array(values(2), values(4))));
    context.write(new IntArrayWritable(Array(values(2), values(3))), new IntArrayWritable(Array(values(0), values(4))));
  }
}

class MergeReducer extends Reducer[IntArrayWritable, IntArrayWritable, Text, Text] {
  // Input : key : src, src weight,  values: dest, edge weight
  override def reduce(key: IntArrayWritable, values: java.lang.Iterable[IntArrayWritable],
    context: Reducer[IntArrayWritable, IntArrayWritable, Text, Text]#Context) = {

    /*
    values.foreach(value => {
      // dest_id, edge_weight
      var intval = value.get.map(_.toString.toInt);
      context.write(new Text(key.get.map(_.toString).mkString("\t")), new Text(intval.mkString("\t")));
    });
    * 
    */
    var vals = scala.collection.mutable.HashSet[(Int, Int)]();

    values.foreach(value => {
      var part = value.get.map(_.toString.toInt);
      vals += ((part(0), part(1)))
    });

    var sbu = new StringBuilder();
    vals.foreach(value => {
      sbu.append("%d\t%d\t".format(value._1, value._2));
    });
    context.write(new Text(key.get.map(_.toString).mkString("\t")), new Text(sbu.toString));
  }
}