package edu.uchicago.cs.dbp.stg2.itconvert

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConversions._
import scala.io.Source
import edu.clarkson.cs.itop.core.parser.Parser
import java.io.FileOutputStream
import java.io.PrintWriter
import edu.clarkson.cs.itop.core.model.Link

object ITConverter extends App {

  var parser = new Parser();
  var output = new PrintWriter(new FileOutputStream("/home/harper/working/link_node"));
  Source.fromFile("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/kapar-midar-iff.links").getLines().foreach { line =>
    {
      if (!line.startsWith("#")) {
        var link = parser.parse[Link](line);
        output.println(link.id + " " + link.nodes.map(_.id).mkString(" "));
      }
    }
  };
  output.close();
}

class LinkReadMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  override def map(key: Object, value: Text,
    context: Mapper[Object, Text, IntWritable, IntWritable]#Context) = {
    var values = value.toString().split("\\s");
    var link = values(0).toInt;
    var nodes = values.slice(1, values.length);

    nodes.foreach { n => context.write(new IntWritable(n.toInt), new IntWritable(link)) };
  }
}

class LinkReadReducer extends Reducer[IntWritable, IntWritable, IntWritable, IntWritable] {

  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable],
    context: Reducer[IntWritable, IntWritable, IntWritable, IntWritable]#Context) = {
    var list = values.toList;
    for (i <- 0 to list.length - 1) {
      for (j <- i + 1 to list.length - 1) {
        if (list(i).get < list(j).get) {
          context.write(list(i), list(j));
        }
        if (list(i).get > list(j).get) {
          context.write(list(j), list(i));
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