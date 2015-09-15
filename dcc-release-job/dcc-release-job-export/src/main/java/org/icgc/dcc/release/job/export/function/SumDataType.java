package org.icgc.dcc.release.job.export.function;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import lombok.val;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple3;

public class SumDataType
    implements
    Function2<Tuple3<Map<ByteBuffer, KeyValue[]>, Long, Integer>, Tuple3<Map<ByteBuffer, KeyValue[]>, Long, Integer>, Tuple3<Map<ByteBuffer, KeyValue[]>, Long, Integer>> {

  @Override
  public Tuple3<Map<ByteBuffer, KeyValue[]>, Long, Integer> call(
      Tuple3<Map<ByteBuffer, KeyValue[]>, Long, Integer> tuple1,
      Tuple3<Map<ByteBuffer, KeyValue[]>, Long, Integer> tuple2) throws Exception {
    Map<ByteBuffer, KeyValue[]> data = new TreeMap<>();
    data.putAll(tuple1._1());
    data.putAll(tuple2._1());
    val totalSize = tuple1._2() + tuple2._2();
    val sum = tuple1._3() + tuple2._3();

    return new Tuple3<>(data, totalSize, sum);
  }

}
