/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.investigate.readrow;

import com.google.api.gax.batching.Batcher;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.opencensus.common.Scope;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.Sampler;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class TracingReadRowIT {
  private static final Logger logger = Logger.getLogger(TracingReadRowIT.class.getName());
  private static final Tracer tracer = Tracing.getTracer();

  // [START configChanges]
  private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
  private static final String INSTANCE_ID = System.getenv("INSTANCE_ID");
  private static final Integer ROWS_COUNT = Integer.getInteger("rows.count", 3);
  // [END configChanges]

  // Refer to table metadata names by byte array in the HBase API
  private static final String TABLE_ID = System.getProperty("table.name", "Hello-Bigtable");
  private static final String FAMILY_ID = "cf1";

  private static void configureOpenCensusExporters(Sampler sampler) throws IOException {
    TraceConfig traceConfig = Tracing.getTraceConfig();

    traceConfig.updateActiveTraceParams(
        traceConfig.getActiveTraceParams().toBuilder().setSampler(sampler).build());

    StackdriverTraceExporter.createAndRegister(
        StackdriverTraceConfiguration.builder().setProjectId(PROJECT_ID).build());

    StackdriverStatsExporter.createAndRegister();

    BigtableDataSettings.enableOpenCensusStats();
    //    RpcViews.registerAllGrpcViews();
    //    RpcViews.registerAllGrpcBasicViews();
  }

  private static void writeRows(BigtableDataClient client, String rowKeyPrefix)
      throws InterruptedException {
    try (Batcher<RowMutationEntry, Void> batcher = client.newBulkMutationBatcher(TABLE_ID);
        Scope writeRowScope = tracer.spanBuilder("verify.write.rows").startScopedSpan()) {
      for (int i = 0; i < ROWS_COUNT; i++) {
        batcher.add(
            RowMutationEntry.create(rowKeyPrefix + "-" + i)
                .setCell(FAMILY_ID, "qualifier", 10_000L, "value"));
      }
    }
  }

  private static void readRows(BigtableDataClient client, String rowKeyPrefix) {
    try (Scope readRowScope = tracer.spanBuilder("verify.read.rows").startScopedSpan()) {
      for (int i = 0; i < ROWS_COUNT; i++) {
        Row row = client.readRow(TABLE_ID, rowKeyPrefix + "-" + i);
        Span span = tracer.getCurrentSpan();
        if (row != null) {
          span.addAnnotation("Row key is : " + row.getKey().toStringUtf8());
        }
      }
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    logger.info("Started TraceReadRow....");

    // For investigation purpose
    configureOpenCensusExporters(Samplers.alwaysSample());

    try (BigtableDataClient client = BigtableDataClient.create(PROJECT_ID, INSTANCE_ID)) {
      String prefix = UUID.randomUUID().toString();

      logger.info("Started writing rows");
      writeRows(client, prefix);

      logger.info("Started reading rows");
      readRows(client, prefix);

      try {
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
