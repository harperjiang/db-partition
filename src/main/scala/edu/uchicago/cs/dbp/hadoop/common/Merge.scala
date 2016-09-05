package edu.uchicago.cs.dbp.hadoop.common

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.NullWritable

class MergeMapper extends Mapper[Object, Text, Text, NullWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, NullWritable]#Context): Unit = {
    context.write(value, NullWritable.get);
  }
}