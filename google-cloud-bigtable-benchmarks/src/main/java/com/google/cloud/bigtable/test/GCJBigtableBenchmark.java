/*
 * Copyright 2019 Google LLC
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
package com.google.cloud.bigtable.test;

import com.google.api.gax.batching.Batcher;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@State(Scope.Benchmark)
public class GCJBigtableBenchmark {

  private static final long TOTAL_DATA_IN_BYTES = 1024 * 1024 * 1024;
  private static final Pattern CELL_PATTERN = Pattern.compile("cellsPerRow/(\\d+)/cellSize/(\\d+)");

  private static final String COL_FAMILY = "bm-test-cf";
  private static final String ROW_PREFIX = "bm_row_key-";
  private static final long SAMPLE_TIMESTAMP = 1571940889000L;

  @Param("")
  private String projectId;

  @Param("")
  private String instanceId;

  @Param({"cellsPerRow/1/cellSize/1024"})
  private String rowShape;

  private RowShapeParams rowShapeParams;
  private BigtableDataClient dataClient;

  @Setup
  public void setUp() throws IOException, InterruptedException {
    rowShapeParams = new RowShapeParams(rowShape);
    BigtableTableAdminClient adminClient = BigtableTableAdminClient.create(projectId, instanceId);
    if (!adminClient.exists(rowShapeParams.tableId)) {
      adminClient.createTable(CreateTableRequest.of(rowShapeParams.tableId).addFamily(COL_FAMILY));
    }

    dataClient = BigtableDataClient.create(projectId, instanceId);

    ByteString data = getRandomBytes(rowShapeParams.cellSize);
    try (Batcher<RowMutationEntry, Void> batcher =
        dataClient.newBulkMutationBatcher(rowShapeParams.tableId)) {
      for (int rowInd = 0; rowInd < rowShapeParams.totalRows; rowInd++) {
        RowMutationEntry entry =
            RowMutationEntry.create(ROW_PREFIX + String.format("%010d", rowInd));

        for (int cellInd = 0; cellInd < rowShapeParams.cellsPerRow; cellInd++) {
          entry.setCell(
              COL_FAMILY,
              ByteString.copyFromUtf8("qualifier-" + String.format("%06d", cellInd)),
              SAMPLE_TIMESTAMP,
              data);
        }
        batcher.add(entry);
      }
    }
  }

  @Benchmark
  public void bulkQuery(Blackhole blackhole) {
    int totalRow = 0;
    ServerStream<Row> rows =
        dataClient.readRows(Query.create(rowShapeParams.tableId).rowKey(ROW_PREFIX));
    for (Row row : rows) {
      totalRow++;
      for (RowCell cell : row.getCells()) {
        blackhole.consume(cell);
      }
    }
    System.out.println(
        String.format(
            "rowShapeParam: %d and total rows count: %d ", rowShapeParams.totalRows, totalRow));
  }

  static ByteString getRandomBytes(int size) {
    byte[] data = new byte[size];
    new Random().nextBytes(data);
    return ByteString.copyFrom(data);
  }

  static class RowShapeParams {

    final int cellsPerRow;
    final int cellSize;
    final String tableId;
    final long totalRows;

    RowShapeParams(String dataShape) {
      Matcher matcher = CELL_PATTERN.matcher(dataShape);
      Preconditions.checkArgument(matcher.matches(), "Benchmark job configuration did not match");
      cellsPerRow = Integer.valueOf(matcher.group(1));
      cellSize = Integer.valueOf(matcher.group(2));
      Preconditions.checkArgument(
          cellsPerRow != 0 && cellSize != 0, "CellsPerSize or CellSize cannot be zero");

      tableId = getTableId(cellsPerRow, cellSize);

      // Total rows is ~1GB, 15 is the additional size of qualifier, taking this into account is
      // necessary, especially when cellSize is provided 1.
      totalRows = TOTAL_DATA_IN_BYTES / (cellsPerRow * (cellSize + 15));
    }
  }

  private static String getTableId(int cellsPerRow, int cellSize) {
    return String.format("benchmark_table_%d-cells_%d-bytes", cellsPerRow, cellSize);
  }
}
