package edu.uchicago.cs.dbp.eval

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob
import scala.collection.JavaConversions._
import java.util.Collections

object Evaluate extends App {

  var conf = new Configuration()

  var tpj = new ControlledJob(tpJob(conf), Collections.emptyList[ControlledJob]());
  var tpdj = new ControlledJob(tpDetailJob(conf), Collections.emptyList[ControlledJob]());
  var tpcj = new ControlledJob(tpCountJob(conf), List(tpj));
  var tpmsj = new ControlledJob(tpMoveSizeJob(conf), List(tpdj));

  var controller = new JobControl("Evaluator")
  controller.addJob(tpj);
  controller.addJob(tpdj);
  controller.addJob(tpcj);
  controller.addJob(tpmsj);

  controller.run()

  def tpJob(conf: Configuration): Job = {
    var job = Job.getInstance(conf, "Transaction Partition Join");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[TPJoinMapper]);
    job.setReducerClass(classOf[TPJoinReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(""))
    FileOutputFormat.setOutputPath(job, new Path(""))
    return job;
  }

  def tpDetailJob(conf: Configuration): Job = {
    var job = Job.getInstance(conf, "Transaction Partition Detail Join");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[TPJoinMapper]);
    job.setReducerClass(classOf[TPJoinReducer2]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(""))
    FileOutputFormat.setOutputPath(job, new Path(""))
    return job
  }

  def tpCountJob(conf: Configuration): Job = {
    var job = Job.getInstance(conf, "Transaction Partition Counter");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[TPCountMapper]);
    job.setReducerClass(classOf[TPCountReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(""))
    FileOutputFormat.setOutputPath(job, new Path(""))
    return job;
  }

  def tpMoveSizeJob(conf: Configuration): Job = {
    var job = Job.getInstance(conf, "Transaction Partition Move Sizer");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[TPMoveSizeMapper]);
    job.setReducerClass(classOf[TPMoveSizeReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setOutputKeyClass(classOf[IntWritable]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(""))
    FileOutputFormat.setOutputPath(job, new Path(""))
    return job;
  }
}