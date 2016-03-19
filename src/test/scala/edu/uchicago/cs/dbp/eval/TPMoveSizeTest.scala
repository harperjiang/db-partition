package edu.uchicago.cs.dbp.eval

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.junit.Test
import org.junit.Assert._
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.fs.FileSystem
import edu.uchicago.cs.dbp.common.types.StringArrayWritable

class TPMoveSizeTest {
  
  @Test
  def testTpMoveSize() = {
    var conf = new Configuration()
    
    var fs = FileSystem.get(conf);
    fs.delete(new Path("data/test/tpmsz/output"), true);
    
    var job = Job.getInstance(conf, "Transaction Partition Move Sizer");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[TPMoveSizeMapper]);
    job.setReducerClass(classOf[TPMoveSizeReducer]);   
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path("data/test/tpmsz/input"))
    FileOutputFormat.setOutputPath(job, new Path("data/test/tpmsz/output"))
    job.waitForCompletion(true)
    
    assertTrue(FileCompare.compare("data/test/tpmsz/result", "data/test/tpmsz/output/part-r-00000"))
  }
}