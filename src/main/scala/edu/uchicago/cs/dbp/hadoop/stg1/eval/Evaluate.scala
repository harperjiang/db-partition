package edu.uchicago.cs.dbp.hadoop.stg1.eval

import java.util.Collections

import scala.collection.JavaConversions.seqAsJavaList

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import edu.uchicago.cs.dbp.hadoop.common.DistinctMapper
import edu.uchicago.cs.dbp.hadoop.common.DistinctReducer
import edu.uchicago.cs.dbp.hadoop.common.types.KeyGroupComparator.Key2GroupComparator
import edu.uchicago.cs.dbp.hadoop.common.types.KeyPartitioner.Key2Partitioner
import edu.uchicago.cs.dbp.hadoop.common.types.StringArrayWritable
import edu.uchicago.cs.dbp.hadoop.common.CounterParam
import edu.uchicago.cs.dbp.hadoop.common.CounterReducer
import edu.uchicago.cs.dbp.hadoop.common.CounterMapper
import edu.uchicago.cs.dbp.hadoop.stg1.eval.BaseFolder.path

object Evaluate extends App {

  

  var conf = new Configuration()

  // Clear folder
  var fs = FileSystem.get(conf)
  fs.delete(path("output"), true)

  var tpj = new ControlledJob(tpJob(conf), Collections.emptyList[ControlledJob]());
  var tpdj = new ControlledJob(tpDetailJob(conf), Collections.emptyList[ControlledJob]());

  var tpdij = new ControlledJob(tpDistinctJob(conf), List(tpj));
  var tpddij = new ControlledJob(tpDistinctDetailJob(conf), List(tpdj));

  var tpcj = new ControlledJob(tpCountJob(conf), List(tpdij));
  var tpmsj = new ControlledJob(tpMoveSizeJob(conf), List(tpddij));

  var tphistoj = new ControlledJob(tpCountHistoJob(conf),List(tpcj))
  var tpmhistoj = new ControlledJob(tpMoveSizeHistoJob(conf),List(tpmsj));
  
  var controller = new JobControl("Evaluator")
  controller.addJob(tpj);
  controller.addJob(tpdj);

  controller.addJob(tpdij);
  controller.addJob(tpddij);

  controller.addJob(tpcj);
  controller.addJob(tpmsj);
  
  controller.addJob(tphistoj);
  controller.addJob(tpmhistoj);
  
  controller.run()

  def tpJob(conf: Configuration): Job = {
    var job = Job.getInstance(conf, "Transaction Partition Join");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[TPJoinMapper]);
    job.setReducerClass(classOf[TPJoinReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);

    job.setPartitionerClass(classOf[Key2Partitioner]);
    job.setGroupingComparatorClass(classOf[Key2GroupComparator]);

    FileInputFormat.addInputPath(job, path("partition"))
    FileInputFormat.addInputPath(job, path("transaction"))
    FileOutputFormat.setOutputPath(job, path("output/tran_part_raw"));
    return job
  }

  def tpDistinctJob(conf: Configuration): Job = {
    var job = Job.getInstance(conf, "Transaction Partition Distinct");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[DistinctMapper]);
    job.setReducerClass(classOf[DistinctReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[NullWritable]);
    FileInputFormat.addInputPath(job, path("output/tran_part_raw"))
    FileOutputFormat.setOutputPath(job, path("output/tran_part"))
    return job;
  }

  def tpDetailJob(conf: Configuration): Job = {
    var job = Job.getInstance(conf, "Transaction Partition Detail Join");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[TPJoinMapper]);
    job.setReducerClass(classOf[TPJoinReducer2]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);

    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);

    job.setPartitionerClass(classOf[Key2Partitioner]);
    job.setGroupingComparatorClass(classOf[Key2GroupComparator]);

    FileInputFormat.addInputPath(job, path("transaction"))
    FileInputFormat.addInputPath(job, path("partition"))
    FileOutputFormat.setOutputPath(job, path("output/tran_part_detail_raw"))
    return job
  }

  def tpDistinctDetailJob(conf: Configuration): Job = {
    var job = Job.getInstance(conf, "Transaction Partition Detail Distinct");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[DistinctMapper]);
    job.setReducerClass(classOf[DistinctReducer]);
    job.setNumReduceTasks(Params.clusterSize);

    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[NullWritable]);
    FileInputFormat.addInputPath(job, path("output/tran_part_detail_raw"))
    FileOutputFormat.setOutputPath(job, path("output/tran_part_detail"))
    return job;
  }

  def tpCountJob(conf: Configuration): Job = {
    var job = Job.getInstance(conf, "Transaction Partition Counter");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[TPCountMapper]);
    job.setReducerClass(classOf[TPCountReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, path("output/tran_part"))
    FileOutputFormat.setOutputPath(job, path("output/tran_sum"))
    return job;
  }

  def tpCountHistoJob(conf: Configuration): Job = {
    conf.set(CounterParam.KEY_INDEX, "1")
    var job = Job.getInstance(conf, "Transaction Partition Count Histo");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[CounterMapper]);
    job.setReducerClass(classOf[CounterReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, path("output/tran_sum"))
    FileOutputFormat.setOutputPath(job, path("output/tran_sum_histo"))
    return job;
  }

  def tpMoveSizeJob(conf: Configuration): Job = {
    var job = Job.getInstance(conf, "Transaction Partition Move Sizer");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[TPMoveSizeMapper]);
    job.setReducerClass(classOf[TPMoveSizeReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[StringArrayWritable]);
    job.setMapOutputValueClass(classOf[StringArrayWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, path("output/tran_part_detail"))
    FileOutputFormat.setOutputPath(job, path("output/tran_movesize"))
    return job;
  }
  
  def tpMoveSizeHistoJob(conf: Configuration): Job = {
    conf.set(CounterParam.KEY_INDEX, "1")
    var job = Job.getInstance(conf, "Transaction Partition Move Size Histo");
    job.setJarByClass(Evaluate.getClass);
    job.setMapperClass(classOf[CounterMapper]);
    job.setReducerClass(classOf[CounterReducer]);
    job.setNumReduceTasks(Params.clusterSize);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[IntWritable]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, path("output/tran_movesize"))
    FileOutputFormat.setOutputPath(job, path("output/tran_movesize_histo"))
    return job;
  }

}