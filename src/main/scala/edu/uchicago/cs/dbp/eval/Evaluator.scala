package edu.uchicago.cs.dbp.eval

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Reducer

import edu.uchicago.cs.dbp.common.JoinReducer
import edu.uchicago.cs.dbp.common.MultiKeyJoinMapper
import edu.uchicago.cs.dbp.common.TextMapper

/**
 * Input: transaction(left)
 *            transaction_id, data_type, data_id
 * Input: partition(right)
 * 						partition_id, data_type, data_id
 *
 * Output: trans_part
 *            transaction_id, partition_id
 *
 */

class TPJoinMapper extends MultiKeyJoinMapper("transaction", "partition", Array(1, 2), Array(1, 2));

class TPJoinReducer extends JoinReducer(
  null,
  (key: Array[Writable], left: Array[Writable], right: Array[Writable]) => {
    (new Text(left(0).toString), new Text(right(0).toString))
  });

/**
 * Output: trans_part_detail
 * 					transaction_id, partition_id, data_type, data_id
 */
class TPJoinReducer2 extends JoinReducer(
  null,
  (key: Array[Writable], left: Array[Writable], right: Array[Writable]) => {
    (new Text(left(0).toString), new Text((Array(right(0)) ++ key).mkString("\t")))
  });

/**
 * Input: trans_part
 * 						transaction_id, partition_id
 * Output: trans_sum
 * 						transaction_id, num_partition ( > 1)
 */
class TPCountMapper extends TextMapper(Array(0));

class TPCountReducer extends Reducer[Array[Text], Array[Text], Text, IntWritable] {
  override def reduce(key: Array[Text], vals: java.lang.Iterable[Array[Text]],
    context: Reducer[Array[Text], Array[Text], Text, IntWritable]#Context) = {

    var tid = key(0)
    var sum = 0
    vals.foreach(value => { sum += 1 });
    if (sum > 1) {
      context.write(tid, new IntWritable(sum))
    }
  }
}

/**
 * Input: trans_part_detail
 * 					transaction_id, partition_id, data_type, data_id
 * Output: trans_move_size
 * 					transaction_id, data_size
 */
class TPMoveSizeMapper extends TextMapper(Array(0));

class TPMoveSizeReducer extends Reducer[Array[Writable], Array[Writable], Text, IntWritable] {
  override def reduce(key: Array[Writable], values: java.lang.Iterable[Array[Writable]],
    context: Reducer[Array[Writable], Array[Writable], Text, IntWritable]#Context) = {

    var pmap = new scala.collection.mutable.HashMap[String, Int]()
    var sizer = new DefaultDataSizer()

    values.foreach {
      value =>
        {
          var partition = value(0).toString()
          var datatype = value(1).toString
          var cursize = pmap.getOrElseUpdate(partition, 0)
          cursize += sizer.size(datatype)
          pmap.put(partition, cursize)
        }
    }

    var sum = pmap.values.sum
    var min = pmap.values.min

    context.write(new Text(key(0).toString), new IntWritable(sum - min))
  }
}