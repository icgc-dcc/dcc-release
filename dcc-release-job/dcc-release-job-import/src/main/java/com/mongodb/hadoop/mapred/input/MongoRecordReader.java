// MongoRecordReader.java
/*
 * Copyright 2010 10gen Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.mapred.input;

import lombok.val;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.bson.BSONObject;

import com.google.common.collect.ImmutableSet;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoException;
import com.mongodb.hadoop.io.BSONWritable;

/**
 * "Injecting" this implementation to fix a bug in the original class. next() does not clean the value object before
 * putting new values. This causes the result to contain extra fields from the previous method call.
 */
@SuppressWarnings("deprecation")
public class MongoRecordReader implements RecordReader<BSONWritable, BSONWritable> {

  public MongoRecordReader(MongoInputSplit split) {
    _cursor = split.getCursor();
  }

  @Override
  public void close() {
    if (_cursor != null) _cursor.close();
  }

  @Override
  public BSONWritable createKey() {
    return new BSONWritable();
  }

  @Override
  public BSONWritable createValue() {
    return new BSONWritable();
  }

  public BSONObject getCurrentKey() {
    return new BasicDBObject("_id", _current.get("_id"));
  }

  public BSONObject getCurrentValue() {
    return _current;
  }

  @Override
  public float getProgress() {
    try {
      if (_cursor.hasNext()) {
        return 0.0f;
      }
      else {
        return 1.0f;
      }
    } catch (MongoException e) {
      return 1.0f;
    }
  }

  @Override
  public long getPos() {
    return 0; // no progress to be reported, just working on it
  }

  public void initialize(InputSplit split, TaskAttemptContext context) {
  }

  public boolean nextKeyValue() {
    try {
      if (!_cursor.hasNext()) return false;

      _current = _cursor.next();

      return true;
    } catch (MongoException e) {
      return false;
    }
  }

  @Override
  public boolean next(BSONWritable key, BSONWritable value) {
    if (nextKeyValue()) {
      LOG.debug("Had another k/v");
      key.put("_id", getCurrentKey().get("_id"));
      removeAll(value);
      value.putAll(getCurrentValue());
      return true;
    }
    else {
      LOG.info("Cursor exhausted.");
      return false;
    }
  }

  private static void removeAll(BSONWritable value) {
    val fieldNames = ImmutableSet.copyOf(value.keySet());
    fieldNames.stream()
        .forEach(fieldName -> value.removeField(fieldName));
  }

  private final DBCursor _cursor;
  private BSONObject _current;

  private static final Log LOG = LogFactory.getLog(MongoRecordReader.class);
}
