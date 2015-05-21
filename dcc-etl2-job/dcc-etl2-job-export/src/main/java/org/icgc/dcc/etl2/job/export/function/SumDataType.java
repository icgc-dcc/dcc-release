package org.icgc.dcc.etl2.job.export.function;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple3;


public class SumDataType implements Function2<Tuple3<KeyValue[], Long, Integer>, Tuple3<KeyValue[], Long, Integer>, Tuple3<KeyValue[], Long, Integer>> {

  @Override
  public Tuple3<KeyValue[], Long, Integer> call(Tuple3<KeyValue[], Long, Integer> tuple1, Tuple3<KeyValue[], Long, Integer> tuple2) throws Exception {
    KeyValue[] kvs = concat(tuple1._1(), tuple2._1());
    long totalSize = tuple1._2() + tuple2._2();
    int sum = tuple1._3() + tuple2._3();
    return new Tuple3<KeyValue[], Long, Integer>(kvs, totalSize, sum);
  }

  private KeyValue[] concat(KeyValue[] a, KeyValue[] b) {
    int aLen = a.length;
    int bLen = b.length;
    KeyValue[] c= new KeyValue[aLen+bLen];
    System.arraycopy(a, 0, c, 0, aLen);
    System.arraycopy(b, 0, c, aLen, bLen);
    return c;
  }
}
