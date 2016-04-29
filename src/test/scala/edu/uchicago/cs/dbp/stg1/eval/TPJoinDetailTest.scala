package edu.uchicago.cs.dbp.stg1.eval

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Assert.assertTrue
import org.junit.Test

import edu.uchicago.cs.dbp.common.types.KeyGroupComparator.Key2GroupComparator
import edu.uchicago.cs.dbp.common.types.KeyPartitioner.Key2Partitioner
import edu.uchicago.cs.dbp.common.types.StringArrayWritable
import edu.uchicago.cs.dbp.stg1.eval.Params;

class TPJoinDetailTest {
  @Test
  def testJoinDetail(): Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    fs.delete(new Path("data/test/tpjoind/output"), true);

    var job = Job.getInstance(conf, "Transaction Partition Detail Join");
    job.setJarByClass(classOf[TPJoinTest]);
    job.setMapperClass(classOf[TPJoinMapper]);
    job.setReducerClass(classOf[TPJoinReducer2]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);

    job.setPartitionerClass(classOf[Key2Partitioner]);
    job.setGroupingComparatorClass(classOf[Key2GroupComparator]);

    FileInputFormat.addInputPath(job, new Path("data/test/tpjoind/transaction"))
    FileInputFormat.addInputPath(job, new Path("data/test/tpjoind/partition"))
    FileOutputFormat.setOutputPath(job, new Path("data/test/tpjoind/output"))
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("data/test/tpjoind/result", "data/test/tpjoind/output/part-r-00000"))
  }
}