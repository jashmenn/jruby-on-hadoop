/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ruby;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/* output to a subdirectory according to the key */
class KeyBasedMultipleTextOutputFormat extends MultipleTextOutputFormat<Text, Text> {
  protected String generateFileNameForKeyValue(Text key, Text v, String name) {
    return key.toString() + "/" + name;
  }

  protected Text generateActualKey(Text key, Text value) {
    String[] fields = value.toString().split("\t");
    return new Text(fields[0]);
  }

  protected Text generateActualValue(Text key, Text value) {
    String[] fields = value.toString().split("\t");
    String[] newFields = new String[fields.length - 1];
    System.arraycopy(fields, 1, newFields, 0, fields.length - 1);
    String out = join(newFields, "\t");
    return new Text(out);
  }

  // wtf - replace with StringUtils
  String join(String[] s, String glue)
  {
    int k=s.length;
    if (k==0)
      return null;
    StringBuilder out=new StringBuilder();
    out.append(s[0]);
    for (int x=1;x<k;++x)
      out.append(glue).append(s[x]);
    return out.toString();
  }
}
