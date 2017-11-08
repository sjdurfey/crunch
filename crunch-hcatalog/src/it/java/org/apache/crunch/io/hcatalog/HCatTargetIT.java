package org.apache.crunch.io.hcatalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Durfey
 */
public class HCatTargetIT extends CrunchTestSupport {

  HiveConf hconf;

  IMetaStoreClient client;

  @Rule
  public TestName testName = new TestName();
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = tempDir.getDefaultConfiguration();
    // set the warehouse location to the location of the temp dir, so managed
    // tables
    // return a size estimate
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.toString(), tempDir.getRootPath().toString());
    conf.set("datanucleus.schema.autoCreateTables", "true");
    conf.set("hive.metastore.schema.verification", "false");
    hconf = HCatUtil.getHiveConf(conf);
    client = HCatUtil.getHiveMetastoreClient(hconf);
  }

  @After
  public void tearDown() throws Exception {
    client.close();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path("metastore_db"), true);
    fs.delete(new Path("derby.log"), false);
  }

  @Test
  public void test_successfulWriteToHCatTarget() throws IOException, HiveException, TException {
    String tableName = testName.getMethodName();
    Path tableRootLocation = tempDir.getPath(tableName);
    String data = "17,josh\n29,indiana\n";
    writeDataToHdfs(data, tableRootLocation, conf);

    FieldSchema partitionSchema = new FieldSchema();
    partitionSchema.setName("timestamp");
    partitionSchema.setType("string");
    HCatTestUtils.createTable(client, "default", tableName, TableType.EXTERNAL_TABLE, tableRootLocation,
        Lists.newArrayList(partitionSchema));

    Pipeline pipeline = new MRPipeline(HCatSourceIT.class, tempDir.getDefaultConfiguration());
    PCollection<String> contents = pipeline.readTextFile(tableRootLocation.toString());
    PCollection<HCatRecord> hcatRecords = contents.parallelDo(new HCatTestUtils.Fns.MapHCatRecordFn(),
        Writables.writables(HCatRecord.class));
    Map<String, String> partitions = new HashMap<String, String>() {
      {
        {
          put("timestamp", "1234");
        }
      }
    };

    HCatTarget target = new HCatTarget("default", tableName, partitions);

    pipeline.write(hcatRecords, target);
    pipeline.run();

    // ensure partition was created
    List<Partition> partitionList = client.listPartitions("default", tableName, (short) 5);
    assertThat(partitionList.size(), is(1));
    Partition newPartition = Iterators.getOnlyElement(partitionList.iterator());
    assertThat(newPartition.getValuesIterator().next(), is("1234"));

    // read data from table to ensure it was written correctly
    HCatSource source = new HCatSource("default", tableName, "timestamp='1234'");
    PCollection<HCatRecord> read = pipeline.read(source);
    HCatSchema schema = source.getTableSchema(pipeline.getConfiguration());
    ArrayList<Pair<Integer, String>> mat = Lists.newArrayList(
        read.parallelDo(new HCatTestUtils.Fns.MapPairFn(schema), Avros.tableOf(Avros.ints(), Avros.strings()))
            .materialize());
    assertEquals(ImmutableList.of(Pair.of(17, "josh"), Pair.of(29, "indiana")), mat);
    partitions = new HashMap<String, String>() {
      {
        {
          put("timestamp", "5678");
        }
      }
    };
    target = new HCatTarget("default", tableName, partitions);
    pipeline.write(read, target);
    pipeline.done();
  }

  @Test
  public void test_successfulWriteToHCatTarget_GroupByKey() throws IOException, HiveException, TException {

    String tableName = testName.getMethodName();
    Path tableRootLocation = tempDir.getPath(tableName);
    String data = "17,josh\n29,indiana\n";
    writeDataToHdfs(data, tableRootLocation, conf);

    FieldSchema partitionSchema = new FieldSchema();
    partitionSchema.setName("timestamp");
    partitionSchema.setType("string");
    HCatTestUtils.createTable(client, "default", tableName, TableType.EXTERNAL_TABLE, tableRootLocation,
        Lists.newArrayList(partitionSchema));

    Pipeline pipeline = new MRPipeline(HCatSourceIT.class, conf);
    PCollection<String> contents = pipeline.readTextFile(tableRootLocation.toString());
    PCollection<HCatRecord> hcatRecords = contents.parallelDo(new HCatTestUtils.Fns.MapHCatRecordFn(),
        Writables.writables(HCatRecord.class));
    Map<String, String> partitions = new HashMap<String, String>() {
      {
        {
          put("timestamp", "1234");
        }
      }
    };

    HCatTarget target = new HCatTarget(tableName, partitions);

    pipeline.write(hcatRecords, target);
    pipeline.run();

    // ensure partition was created
    List<Partition> partitionList = client.listPartitions("default", tableName, (short) 5);
    assertThat(partitionList.size(), is(1));
    Partition newPartition = Iterators.getOnlyElement(partitionList.iterator());
    assertThat(newPartition.getValuesIterator().next(), is("1234"));

    // read data from table to ensure it was written correctly
    HCatSource source = new HCatSource("default", tableName, "timestamp='1234'");
    PCollection<HCatRecord> read = pipeline.read(source);
    HCatSchema schema = source.getTableSchema(pipeline.getConfiguration());
    PGroupedTable<String, DefaultHCatRecord> table = read.parallelDo(new HCatTestUtils.Fns.GroupByHCatRecordFn(),
        Writables.tableOf(Writables.strings(), Writables.writables(DefaultHCatRecord.class))).groupByKey();

    Iterable<Pair<Integer, String>> mat = table
        .parallelDo(new HCatTestUtils.Fns.IterableToHCatRecordMapFn(), Writables.writables(HCatRecord.class))
        .parallelDo(new HCatTestUtils.Fns.MapPairFn(schema), Avros.tableOf(Avros.ints(), Avros.strings()))
        .materialize();

    assertEquals(ImmutableList.of(Pair.of(29, "indiana"), Pair.of(17, "josh")), ImmutableList.copyOf(mat));
    pipeline.done();
  }

  @Test
  public void test_HCatTarget_WriteToNonNativeTable_HBase() throws Exception {
    HBaseTestingUtility hbaseTestUtil = null;
    try {
      String db = "default";
      String sourceHiveTable = "source_table";
      String destinationHiveTable = "dest_table";
      Configuration conf = HBaseConfiguration.create(tempDir.getDefaultConfiguration());
      hbaseTestUtil = new HBaseTestingUtility(conf);
      hbaseTestUtil.startMiniZKCluster();
      hbaseTestUtil.startMiniHBaseCluster(1, 1);

      org.apache.hadoop.hbase.client.Table sourceTable = hbaseTestUtil.createTable(TableName.valueOf(sourceHiveTable),
          "fam");

      String key1 = "this-is-a-key";
      Put put = new Put(Bytes.toBytes(key1));
      put.addColumn("fam".getBytes(), "foo".getBytes(), "17".getBytes());
      sourceTable.put(put);
      String key2 = "this-is-a-key-too";
      Put put2 = new Put(Bytes.toBytes(key2));
      put2.addColumn("fam".getBytes(), "foo".getBytes(), "29".getBytes());
      sourceTable.put(put2);
      sourceTable.close();

      // create Hive Table for source table
      org.apache.hadoop.hive.ql.metadata.Table tbl = new org.apache.hadoop.hive.ql.metadata.Table(db, sourceHiveTable);
      tbl.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      tbl.setTableType(TableType.EXTERNAL_TABLE);

      FieldSchema f1 = new FieldSchema();
      f1.setName("foo");
      f1.setType("int");
      FieldSchema f2 = new FieldSchema();
      f2.setName("key");
      f2.setType("string");

      tbl.setProperty("storage_handler", "org.apache.hadoop.hive.hbase.HBaseStorageHandler");
      tbl.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
      tbl.setFields(ImmutableList.of(f1, f2));
      tbl.setSerdeParam("hbase.columns.mapping", "fam:foo,:key");
      this.client.createTable(tbl.getTTable());

      // creates destination table
      hbaseTestUtil.createTable(TableName.valueOf(destinationHiveTable), "fam");
      org.apache.hadoop.hive.ql.metadata.Table destTable = new org.apache.hadoop.hive.ql.metadata.Table(db,
          destinationHiveTable);
      destTable.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      destTable.setTableType(TableType.EXTERNAL_TABLE);

      destTable.setProperty("storage_handler", "org.apache.hadoop.hive.hbase.HBaseStorageHandler");
      destTable.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
      destTable.setFields(ImmutableList.of(f1, f2));
      destTable.setSerdeParam("hbase.columns.mapping", "fam:foo,:key");
      this.client.createTable(destTable.getTTable());

      Pipeline p = new MRPipeline(HCatSourceIT.class, conf);
      HCatSource src = new HCatSource(sourceHiveTable);
      PCollection<HCatRecord> records = p.read(src);
      HCatTarget hCatTarget = new HCatTarget(destinationHiveTable);
      p.write(records, hCatTarget);
      p.done();

      Connection connection = null;
      try {
        Scan scan = new Scan();

        connection = ConnectionFactory.createConnection(conf);
        org.apache.hadoop.hbase.client.Table table = connection.getTable(TableName.valueOf(destinationHiveTable));
        ResultScanner scanner = table.getScanner(scan);

        Result result = null;
        List<Pair<String, Integer>> actual = new ArrayList<>();
        while (((result = scanner.next()) != null)) {
          String value = Bytes.toString(result.getValue("fam".getBytes(), "foo".getBytes()));
          actual.add(Pair.of(Bytes.toString(result.getRow()), Integer.parseInt(value)));
        }

        Assert.assertEquals(ImmutableList.of(Pair.of(key1, 17), Pair.of(key2, 29)), actual);
      } finally {
        IOUtils.closeQuietly(connection);
      }
    } finally {
      if (hbaseTestUtil != null) {
        hbaseTestUtil.shutdownMiniHBaseCluster();
        hbaseTestUtil.shutdownMiniZKCluster();
      }
    }
  }

  // writes data to the specified location and ensures the directory exists
  // prior to writing
  private Path writeDataToHdfs(String data, Path location, Configuration conf) throws IOException {
    FileSystem fs = location.getFileSystem(conf);
    Path writeLocation = new Path(location, UUID.randomUUID().toString());
    fs.mkdirs(location);
    fs.create(writeLocation);
    ByteArrayInputStream baos = new ByteArrayInputStream(data.getBytes("UTF-8"));
    try (FSDataOutputStream fos = fs.create(writeLocation)) {
      IOUtils.copy(baos, fos);
    }

    return writeLocation;
  }
}
