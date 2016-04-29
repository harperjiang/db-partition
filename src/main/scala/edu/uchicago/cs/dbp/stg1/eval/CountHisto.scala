package edu.uchicago.cs.dbp.stg1.eval

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import edu.uchicago.cs.dbp.common.CounterParam
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import edu.uchicago.cs.dbp.common.CounterReducer
import edu.uchicago.cs.dbp.common.CounterMapper
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
object CountHisto extends App {

  var conf = new Configuration()

  // Clear folder
  var fs = FileSystem.get(conf)
  fs.delete(BaseFolder.path("histo"), true)

  var job = tpHistoJob(conf)
  job.waitForCompletion(true)

  def tpHistoJob(conf: Configuration): Job = {
    conf.set(CounterParam.KEY_INDEX, "0")
    var job = Job.getInstance(conf, "Partition Size Histo");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[CounterMapper]);
    job.setReducerClass(classOf[CounterReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, BaseFolder.path("partition"))
    FileOutputFormat.setOutputPath(job, BaseFolder.path("histo/partition_size"))
    return job;
  }
}