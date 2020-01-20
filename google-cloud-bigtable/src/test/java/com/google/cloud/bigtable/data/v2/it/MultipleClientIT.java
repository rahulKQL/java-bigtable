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
package com.google.cloud.bigtable.data.v2.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.batching.Batcher;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClientFactory;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MultipleClientIT {

  private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
  private static final String INSTANCE_ID = "connectors";

  private static final String TABLE_ID = "Hello-Bigtable";
  private static final String COL_FAMILY = "cf1";
  private static BigtableDataClientFactory factory;

  private static String VALUE = RandomStringUtils.random(200);

  @BeforeClass
  public static void setUp() throws IOException {
    BigtableDataSettings settings =
        BigtableDataSettings.newBuilder()
            .setProjectId(PROJECT_ID)
            .setInstanceId(INSTANCE_ID)
            .build();
    factory = BigtableDataClientFactory.create(settings);
  }

  @Test
  @Ignore
  public void testWithOneClient() throws Exception {
    BigtableDataClient client = factory.createDefault();
    readRows(client, null);

    System.out.println(generateThreadDump());

    batcher(client);

    client.close();
  }

  @Test
  @Ignore
  public void testWithMultipleClient() throws Exception {
    BigtableDataClient bigtableTestClient =
        factory.createForInstance(PROJECT_ID, "bigtableio-test");
    readRows(bigtableTestClient, null);
    // bigtableTestClient.close();
    System.out.println("bitableIO test client");

    Exception ex = new Exception();

    assertThat(ex).hasMessageThat().contains(" YOUR MESSAGE TO MATCH AGAINST");
    BigtableDataClient enduranceClient = factory.createForInstance(PROJECT_ID, "endurance");
    readRows(enduranceClient, null);
    // enduranceClient.close();
    System.out.println("endurance client end");

    BigtableDataClient laljiTestClient =
        factory.createForInstance(PROJECT_ID, "lalji-test-instance");
    readRows(laljiTestClient, null);
    // laljiTestClient.close();
    System.out.println("lalji client");

    System.out.println(generateThreadDump());
  }

  private static void readRows(BigtableDataClient client, String rowKey) {
    Query query = Query.create(TABLE_ID).limit(1);
    if (rowKey != null && !rowKey.isEmpty()) {
      query.rowKey(rowKey);
    }

    ServerStream<Row> stream = client.readRows(query);

    for (Row row : stream) {
      printRow(row);
    }
    System.out.println("\t\t------");
  }

  private static void printRow(Row row) {
    String rowK = row.getKey().toStringUtf8();
    print("RowKey: " + rowK);
    for (RowCell rc : row.getCells()) {
      print(
          String.format(
              "\t Family: %s, Qualifier: %s, Value: %s\n",
              rc.getFamily(), rc.getQualifier(), rc.getValue()));
    }
  }

  private static void mutateRow(BigtableDataClient client, String rowKey) {
    String qualifier = RandomStringUtils.random(5);

    client.mutateRow(
        RowMutation.create(TABLE_ID, rowKey)
            .setCell(COL_FAMILY, qualifier, "This is temporary value"));
    print("\t\t------");
  }

  private static void sampling(BigtableDataClient client) {
    List<KeyOffset> offsetList = client.sampleRowKeys(TABLE_ID);
    for (KeyOffset keyOffset : offsetList) {
      print(keyOffset.getKey().toStringUtf8());
      print(keyOffset.toString());
    }
  }

  private static void checkAndMutate(BigtableDataClient client, String rowKey) {
    String qualifier = "new-qual-" + RandomStringUtils.random(5);
    client.checkAndMutateRow(
        ConditionalRowMutation.create(TABLE_ID, rowKey)
            .then(Mutation.create().setCell(COL_FAMILY, qualifier, "Added another value"))
            .otherwise(
                Mutation.create()
                    .setCell(
                        COL_FAMILY,
                        qualifier,
                        "Fresh Row added, this is first value in the table")));
    print("Done checkAndMutate");
  }

  private static void readModifyWrite(BigtableDataClient client, String rowKey) {
    client.readModifyWriteRow(
        ReadModifyWriteRow.create(TABLE_ID, rowKey)
            .append(
                COL_FAMILY,
                "I don't know this",
                "This is a value added with readModifyRow but I don't know what this does"));
    print("Done readModifyWrite");
  }

  /*

  cbt -instance=gcloud-tests-instance-0675bdc0 createtable 'Hello-Bigtable'
  cbt -instance=gcloud-tests-instance-0675bdc0 createfamily 'Hello-Bigtable' cf1
  cbt -instance='gcloud-tests-instance-0675bdc0' set 'Hello-Bigtable' 'second-row'  'cf1':'first-column'='Welcome to the  Bigtable, You are in gcloud-tests-instance-0675bdc0 instance'
   */

  private static String generateThreadDump() {
    final StringBuilder dump = new StringBuilder();
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final ThreadInfo[] threadInfos =
        threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
    for (ThreadInfo threadInfo : threadInfos) {
      dump.append('"');
      dump.append(threadInfo.getThreadName());
      dump.append("\" ");
      final Thread.State state = threadInfo.getThreadState();
      dump.append("\n   java.lang.Thread.State: ");
      dump.append(state);
      final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
      for (final StackTraceElement stackTraceElement : stackTraceElements) {
        dump.append("\n        at ");
        dump.append(stackTraceElement);
      }
      dump.append("\n\n");
    }
    return dump.toString();
  }

  @Test
  @Ignore
  public void testWithSequential() {
    BigtableDataClient client = factory.createDefault();

    String rowKey = "TOn17Jan2020-" + RandomStringUtils.random(5);

    sampling(client);

    mutateRow(client, rowKey);
    readRows(client, rowKey);

    checkAndMutate(client, rowKey);
    readRows(client, rowKey);

    readModifyWrite(client, rowKey);
    readRows(client, rowKey);
  }

  @Test
  @Ignore
  public void testWithParallel() throws InterruptedException {
    final ExecutorService EXECUTOR = Executors.newFixedThreadPool(8);
    final BigtableDataClient client = factory.createDefault();

    final String rowKey = "TOn17Jan2020-" + RandomStringUtils.random(5);

    EXECUTOR.submit(
        new Callable() {

          @Override
          public Void call() {
            sampling(client);
            return null;
          }
        });

    EXECUTOR.submit(
        new Callable() {

          @Override
          public Void call() {
            mutateRow(client, rowKey);
            return null;
          }
        });

    EXECUTOR.submit(
        new Callable() {

          @Override
          public Void call() {
            readRows(client, rowKey);
            return null;
          }
        });
    EXECUTOR.submit(
        new Callable() {

          @Override
          public Void call() {
            checkAndMutate(client, rowKey);
            return null;
          }
        });
    EXECUTOR.submit(
        new Callable() {

          @Override
          public Void call() {
            System.out.println("Again print");
            readRows(client, rowKey);
            readModifyWrite(client, rowKey);
            return null;
          }
        });

    EXECUTOR.awaitTermination(100, TimeUnit.SECONDS);
    System.out.println(generateThreadDump());
    client.close();
  }

  private static void print(String lineToPrint) {
    System.out.println(lineToPrint);
  }

  @Test
  @Ignore
  public void testWithLongRunningOps() throws Exception {
    final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
    final BigtableDataClient client = factory.createDefault();

    Future task1 =
        EXECUTOR.submit(
            new Callable() {
              @Override
              public Void call() {
                try {
                  batcher(client);
                } catch (Exception e) {
                  print("EXCEPTION in testWithLongRunningOps#batcher");
                  e.printStackTrace();
                }
                return null;
              }
            });

    Future task2 =
        EXECUTOR.submit(
            new Callable() {
              @Override
              public Void call() {
                try {
                  longRunningRead(client);
                } catch (Exception e) {
                  print("EXCEPTION in testWithLongRunningOps#longRunningRead");
                  e.printStackTrace();
                }
                return null;
              }
            });

    System.out.println("task1.isDone(): " + task1.isDone());
    System.out.println("task2.isDone(): " + task2.isDone());

    System.out.println("Both tasks are executed");
    EXECUTOR.awaitTermination(100, TimeUnit.SECONDS);

    System.out.println("task1.isDone(): " + task1.isDone());
    System.out.println("task2.isDone(): " + task2.isDone());

    client.close();
    EXECUTOR.shutdown();
  }

  private static void batcher(BigtableDataClient client) throws Exception {
    Batcher<RowMutationEntry, Void> batcher = client.newBulkMutationBatcher(TABLE_ID);
    for (int i = 0; i < 400; i++) {
      String rowKey = "TOn17Jan2020-" + RandomStringUtils.random(3);
      batcher.add(
          RowMutationEntry.create(rowKey)
              .setCell(COL_FAMILY, "qual-" + String.format("%08d", i), VALUE));

      if (i % 30 == 0) {
        TimeUnit.SECONDS.sleep(3);
        print("One Batch of Batching Completed");
      }
    }
  }

  private static void longRunningRead(BigtableDataClient client) throws Exception {
    ServerStream<Row> rows = client.readRows(Query.create(TABLE_ID).limit(200));
    int i = 0;
    for (Row row : rows) {
      System.out.println("RowKey: " + row.getKey());

      i++;
      if (i % 100 == 0) {
        TimeUnit.SECONDS.sleep(2);
        print("One Batch of Read Completed");
      }
    }
  }

  @Test
  public void testMultipleConn() throws Exception {
    final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
    final BigtableDataClient client1 = factory.createDefault();
    readRows(client1, null);

    final BigtableDataClient client2 = factory.createForInstance(PROJECT_ID, "bigtableio-test");
    readRows(client2, null);

    final BigtableDataClient client3 = factory.createForInstance(PROJECT_ID, "endurance");
    readRows(client3, null);

    final BigtableDataClient client4 = factory.createForInstance(PROJECT_ID, "lalji-test-instance");
    readRows(client4, null);

    final BigtableDataClient client5 =
        factory.createForInstance(PROJECT_ID, "gcloud-tests-instance-0675bdc0");
    readRows(client5, null);

    EXECUTOR.execute(
        new Runnable() {
          @Override
          public void run() {
            readRows(client1, null);
          }
        });
    EXECUTOR.execute(
        new Runnable() {
          @Override
          public void run() {
            readRows(client2, null);
          }
        });
    EXECUTOR.execute(
        new Runnable() {
          @Override
          public void run() {
            readRows(client3, null);
          }
        });
    EXECUTOR.execute(
        new Runnable() {
          @Override
          public void run() {
            readRows(client4, null);
          }
        });
    EXECUTOR.execute(
        new Runnable() {
          @Override
          public void run() {
            readRows(client5, null);
          }
        });

    System.out.println(generateThreadDump());

    System.out.println("____________#####__________");
    EXECUTOR.awaitTermination(40, TimeUnit.SECONDS);
    System.out.println(generateThreadDump());
    client1.close();
    client2.close();
    client3.close();
    client4.close();
    client5.close();
  }
}
