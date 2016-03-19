package edu.uchicago.cs.dbp.eval

import org.junit.Test
import org.junit.Assert._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.conf.Configuration
import org.apache.commons.io.FileUtils
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import edu.uchicago.cs.dbp.common.types.StringArrayWritable
import edu.uchicago.cs.dbp.common.types.KeyPartitioner
import edu.uchicago.cs.dbp.common.types.KeyGroupComparator
import edu.uchicago.cs.dbp.common.types.KeyPartitioner.Key2Partitioner
import edu.uchicago.cs.dbp.common.types.KeyGroupComparator.Key2GroupComparator

class TPJoinTest {

  @Test
  def testJoin(): Unit = {
    var conf = new Configuration();

    var fs = FileSystem.get(conf);
    fs.delete(new Path("data/test/tpjoin/output"), true);

    var job = Job.getInstance(conf, "Transaction Partition Join");
    job.setJarByClass(classOf[TPJoinTest]);
    job.setMapperClass(classOf[TPJoinMapper]);
    job.setReducerClass(classOf[TPJoinReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);

    job.setPartitionerClass(classOf[Key2Partitioner]);
    job.setGroupingComparatorClass(classOf[Key2GroupComparator]);

    FileInputFormat.addInputPath(job, new Path("data/test/tpjoin/transaction"))
    FileInputFormat.addInputPath(job, new Path("data/test/tpjoin/partition"))
    FileOutputFormat.setOutputPath(job, new Path("data/test/tpjoin/output"))
    job.waitForCompletion(true);

    assertTrue(FileCompare.compare("data/test/tpjoin/result", "data/test/tpjoin/output/part-r-00000"))
  }
}