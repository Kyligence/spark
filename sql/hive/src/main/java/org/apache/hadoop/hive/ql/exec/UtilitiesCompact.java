/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.JobConf;

import java.util.Map;
import java.util.Properties;

public class UtilitiesCompact {

    public static void copyTableJobPropertiesToConf(TableDesc tbl, JobConf job) {
        Properties tblProperties = tbl.getProperties();
        for (String name : tblProperties.stringPropertyNames()) {
            if (job.get(name) == null) {
                String val = (String) tblProperties.get(name);
                if (val != null) {
                    job.set(name, StringEscapeUtils.escapeJava(val));
                }
            }
        }

        copyLineDelimiterForTextTbl(job, tblProperties);
        Map<String, String> jobProperties = tbl.getJobProperties();
        if (jobProperties != null) {
            for (Map.Entry<String, String> entry : jobProperties.entrySet()) {
                job.set(entry.getKey(), entry.getValue());
            }
        }
    }

    private static void copyLineDelimiterForTextTbl(JobConf job, Properties tblProperties) {
        final String HIVETEXTDELIMITER = "textinputformat.record.delimiter";
        String inputFileFormat = tblProperties.getProperty("file.inputformat");
        if ("org.apache.hadoop.mapred.TextInputFormat".equalsIgnoreCase(inputFileFormat)) {
            String lineDelimiter = tblProperties.getProperty("line.delim");
            if (lineDelimiter != null) {
                job.set(HIVETEXTDELIMITER, lineDelimiter);
            } else {
                job.set(HIVETEXTDELIMITER, "\n");
            }
        }

    }
}
