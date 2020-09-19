/*
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
package com.click.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

    public static interface FileParserOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> value);
    }

  public static void main(String[] args) {

      FileParserOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
              .as(FileParserOptions.class);

    Pipeline p = Pipeline.create(options);

      PCollection<String> records = p.apply(
              "ReadLines", TextIO.read().from(ValueProvider.NestedValueProvider.of(
                      options.getInputFile(),
                      (SerializableFunction<String, String>) file -> file)));

      final TupleTag<String> validRecord =
              new TupleTag<String>(){};

      final TupleTag<String> errorRecord =
              new TupleTag<String>(){};

      records.apply("Record Counter", Count.globally())
              .apply("Logging Count", ParDo.of(new DoFn<Long, Void>() {
                  @ProcessElement
                  public void processElement(@Element Long count) {
                      LOG.error("Total Records Processed: "+count);
                  }
              }));

      PCollectionTuple validatedRecords = records.apply(
        "Validating Records",
        ParDo.of(new DoFn<String, String>() {

            private Boolean isBlank(String value) {
                return (value == null || value.isBlank());
            }

            private Boolean isRecordValid(String recordLine) {
                if (isBlank(recordLine)) {
                    return false;
                }
                String[] fields = recordLine.split("\\|");
                Boolean isRecordDirty = false;
                StringBuilder errorMessageBuilder = new StringBuilder(recordLine);
                errorMessageBuilder.append(" : has errors ");
                if(fields!=null && fields.length > 0) {
                    for (String field : fields) {
                        if(isBlank(field)) {
                            isRecordDirty = true;
                            errorMessageBuilder.append("| Found Empty element; ");
                        } else {
                            String[] fieldKeyValue = field.split(":");
                            if (fieldKeyValue.length == 2) {
                                String key = fieldKeyValue[0];
                                String value = fieldKeyValue[1];
                                if (isBlank(key)) {
                                    isRecordDirty = true;
                                    errorMessageBuilder.append("| Key is blank; ");
                                } else if (isBlank(value)) {
                                    isRecordDirty = true;
                                    errorMessageBuilder.append("| value for " + key + " cannot be blank.; ");
                                }
                            } else {
                                isRecordDirty = true;
                                errorMessageBuilder.append("| Found Empty field; ");
                            }
                        }
                    }
                }
                if(isRecordDirty) {
                    LOG.error(errorMessageBuilder.toString());
                }
                return !isRecordDirty;
            }

            @ProcessElement
            public void processElement(@Element String line, MultiOutputReceiver out) {
                if(isRecordValid(line)) {
                    out.get(validRecord).output(line);
                } else {
                    out.get(errorRecord).output(line);
                }
            }
        }).withOutputTags(validRecord, TupleTagList.of(errorRecord)));

      PCollection<String> validRecords = validatedRecords.get(validRecord);
      PCollection<String> errorRecords = validatedRecords.get(errorRecord);

      errorRecords.apply("Record Counter", Count.globally())
              .apply("Logging Count", ParDo.of(new DoFn<Long, Void>() {
                  @ProcessElement
                  public void processElement(@Element Long count) {
                      if(count!=null && count > 0) {
                          LOG.error("Total Invalid Records : " + count);
                      } else {
                          LOG.error("No Invalid Records :");
                      }
                  }
              }));

      validRecords.apply("TransForm Records",ParDo.of(new DoFn<String, String[]>() {
          @ProcessElement
          public void processElement(@Element String line, OutputReceiver<String[]> out) {
              LOG.info("line content: "+line);
              out.output(line.split("\\|"));
          }

      }));

    p.run();
  }
}
