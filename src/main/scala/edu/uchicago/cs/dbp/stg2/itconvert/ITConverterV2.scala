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
    job.setOutputValueClass(classOf[Text]);

    FileInputFormat.addInputPath(job, new Path("/home/harper/working/link_node"))
    FileOutputFormat.setOutputPath(job, new Path("/home/harper/storage/workingbig/link_link"));
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

class LinkReadV2Reducer extends Reducer[IntWritable, IntArrayWritable, Text, Text] {

  val threshold = 1000;

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
