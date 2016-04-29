package edu.uchicago.cs.dbp.stg1.eval

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Assert.assertTrue
import org.junit.Test

import edu.uchicago.cs.dbp.common.types.StringArrayWritable
import edu.uchicago.cs.dbp.stg1.eval.Evaluate;
import edu.uchicago.cs.dbp.stg1.eval.Params;

class TPCountTest {

  @Test
  def testTpCount() = {
    var conf = new Configuration();
    
    var fs = FileSystem.get(conf);
    fs.delete(new Path("data/test/tpcount/output"), true);
    
    var job = Job.getInstance(conf, "Transaction Partition Counter");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[TPCountMapper]);
    job.setReducerClass(classOf[TPCountReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path("data/test/tpcount/input"))
    FileOutputFormat.setOutputPath(job, new Path("data/test/tpcount/output"))
    job.waitForCompletion(true)
    
    assertTrue(FileCompare.compare("data/test/tpcount/result", "data/test/tpcount/output/part-r-00000"))
  } 
}