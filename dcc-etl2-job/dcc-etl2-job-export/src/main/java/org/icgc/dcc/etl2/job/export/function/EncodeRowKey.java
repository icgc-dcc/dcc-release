package org.icgc.dcc.etl2.job.export.function;

import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.etl2.job.export.util.HTableManager;

import scala.Tuple2;

public class EncodeRowKey implements Function<Tuple2<String, Integer>, byte[]> {

  @Override
  public byte[] call(Tuple2<String, Integer> tuple) throws Exception {
    val donorId = Integer.valueOf(tuple._1());
    val index = tuple._2();
    return HTableManager.encodedRowKey(donorId, index);
  }
}
