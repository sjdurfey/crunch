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
package org.apache.crunch.contrib.hcatalog;

import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.contrib.hcatalog.avro.HCatDecoder;
import org.apache.crunch.contrib.hcatalog.avro.HCatParser;
import org.apache.crunch.io.hcatalog.FromHCat;
import org.apache.crunch.test.Player;
import org.apache.crunch.test.Players;
import org.apache.crunch.test.Position;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.md5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class HCatParserITest {

  private static IMetaStoreClient client;
  private static TemporaryPath temporaryPath;
  private static Configuration conf;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUp() throws Throwable {
    HCatTestSuite.startTest();
    client = HCatTestSuite.getClient();
    temporaryPath = HCatTestSuite.getRootPath();
    conf = HCatTestSuite.getConf();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HCatTestSuite.endTest();
  }

  @Test(expected = IllegalArgumentException.class)
  public void test_NullHCatRecord() {
    new HCatParser(null, Player.getClassSchema());
  }

  @Test(expected = IllegalArgumentException.class)
  public void test_NullWriterSchema() {
    new HCatParser(Mockito.mock(HCatRecord.class), null);
  }

  @Test
  public void test_HCatToSpecificRecord_avroParser() throws IOException, HiveException, TException, SerDeException {
    String tableName = testName.getMethodName();
    Path tableRootLocation = temporaryPath.getPath(tableName);

    Map<CharSequence, CharSequence> teamCities = new HashMap<>();
    teamCities.put(new Utf8("Ohio"), new Utf8("Cleveland"));
    teamCities.put(new Utf8("Missouri"), new Utf8("Kansas City"));
    teamCities.put(new Utf8("Washington"), new Utf8("Seattle"));
    List<CharSequence> teams = new SpecificData.Array<>(2, Player.getClassSchema().getField("previousTeams").schema());
    teams.add(new Utf8("Cleveland Indians"));
    teams.add(new Utf8("Kansas City Royals"));

    byte[] md5 = new byte[16];
    new Random().nextBytes(md5);
    Player player = Player.newBuilder().setBattingAvg(0.310).setName(new Utf8("Francisco Lindor")).setHomeruns(50)
        .setPosition(Position.CenterField).setPreviousTeams(teams).setTeamLocation(teamCities)
        .setTeamLocation(teamCities).setMd5(new md5(md5)).build();

    Map<String, String> tableProps = new HashMap<>();
    tableProps.put("avro.schema.literal", Player.getClassSchema().toString());

    createAvroBackedTable(tableName, tableRootLocation, tableProps);
    File fileName = temporaryPath.getFile("players.avro");
    writeDatum(player, fileName);
    moveDatumsToHdfs(fileName, tableRootLocation, conf);

    Pipeline pipeline = new MRPipeline(HCatSourceITest.class, conf);
    Iterable<HCatRecord> players = pipeline.read(FromHCat.table(tableName)).materialize();

    int datumSize = 0;
    for (final HCatRecord hCatRecord : players) {
      datumSize++;

      SpecificDatumReader<Player> reader = new SpecificDatumReader<>(Player.getClassSchema(), Player.getClassSchema());
      HCatDecoder decoder = new HCatDecoder(hCatRecord, Player.getClassSchema());
      Player read = reader.read(null, decoder);
      assertModel(read, player, true);
    }

    Assert.assertEquals(datumSize, 1);
  }

  // compareFixed indicates if the fixed type should be compared. the test
  // generates
  // random bytes, so if the test generates the bytes itself, compare. if
  // reading
  // from disk, don't compare
  private void assertModel(Player actual, Player expected, boolean compareFixed) {
    if (compareFixed)
      assertThat(actual.getMd5(), is(expected.getMd5()));
    assertThat(actual.getBattingAvg(), is(expected.getBattingAvg()));
    assertThat(actual.getHomeruns(), is(expected.getHomeruns()));
    // assertThat(actual.getItsOptional(), is(expected.getItsOptional()));
    assertThat(actual.getName(), is(expected.getName()));
    assertThat(actual.getPosition(), is(expected.getPosition()));

    assertMaps(actual.getTeamLocation(), expected.getTeamLocation());
    assertThat(actual.getPreviousTeams().size(), is(expected.getPreviousTeams().size()));
    assertTrue(actual.getPreviousTeams().containsAll(expected.getPreviousTeams()));
  }

  private void assertMaps(Map<CharSequence, CharSequence> actual, Map<CharSequence, CharSequence> expected) {
    assertThat(actual.size(), is(expected.size()));
    for (final Map.Entry<CharSequence, CharSequence> entry : expected.entrySet()) {
      CharSequence o = actual.get((entry.getKey()));
      assertThat(o, is(entry.getValue()));
    }
  }

  @Test
  public void test_HCatToAvro_DifferentReaderandWriterSchemas() throws HiveException, IOException, TException {
    String tableName = testName.getMethodName();
    Path tableRootLocation = temporaryPath.getPath(tableName);

    Map<CharSequence, CharSequence> teamCities = new HashMap<>();
    teamCities.put(new Utf8("Ohio"), new Utf8("Cleveland"));
    teamCities.put(new Utf8("Missouri"), new Utf8("Kansas City"));
    teamCities.put(new Utf8("Washington"), new Utf8("Seattle"));
    List<CharSequence> teams = new SpecificData.Array<>(2, Player.getClassSchema().getField("previousTeams").schema());
    teams.add(new Utf8("Cleveland Indians"));
    teams.add(new Utf8("Kansas City Royals"));

    byte[] md5 = new byte[16];
    new Random().nextBytes(md5);
    Player player = Player.newBuilder().setBattingAvg(0.310).setName(new Utf8("Francisco Lindor")).setHomeruns(50)
        .setPosition(Position.CenterField).setPreviousTeams(teams).setTeamLocation(teamCities)
        .setTeamLocation(teamCities).setMd5(new md5(md5)).build();

    File writerSchemaFile = new File("src/it/resources/player_with_optional.avsc");
    Schema writerSchema = new Schema.Parser().parse(writerSchemaFile);

    // create data using a schema with an optional field filled out
    // the reader schema doesn't have this optional field
    GenericData.Record record = new GenericData.Record(writerSchema);
    record.put("position", Position.CenterField.name());
    record.put("name", new Utf8("Francisco Lindor"));
    record.put("homeruns", 50);
    record.put("battingAvg", 0.310);
    record.put("teamLocation", teamCities);
    record.put("previousTeams", teams);
    record.put("itsOptional", "optional");
    record.put("md5", new md5(md5.clone()));

    Map<String, String> tableProps = new HashMap<>();
    tableProps.put("avro.schema.literal", writerSchema.toString());

    createAvroBackedTable(tableName, tableRootLocation, tableProps);
    File fileName = temporaryPath.getFile("players_with_optional_field.avro");
    writeDatum(record, fileName);
    moveDatumsToHdfs(fileName, tableRootLocation, conf);

    Pipeline pipeline = new MRPipeline(HCatSourceITest.class, conf);
    Iterable<HCatRecord> players = pipeline.read(FromHCat.table(tableName)).materialize();

    int datumSize = 0;
    for (final HCatRecord hCatRecord : players) {
      datumSize++;

      SpecificDatumReader<Player> reader = new SpecificDatumReader<>(writerSchema, Player.getClassSchema());
      HCatDecoder decoder = new HCatDecoder(hCatRecord, writerSchema);
      Player read = reader.read(null, decoder);
      assertModel(read, player, false);
    }

    Assert.assertEquals(datumSize, 1);
  }

  @Test
  public void test_HCatDecoder_SkipMap() throws HiveException, IOException, TException {
      String tableName = testName.getMethodName();
      Path tableRootLocation = temporaryPath.getPath(tableName);
    String writerSchemaStr = "{\n"
            + "  \"namespace\": \"org.apache.crunch.test\",\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"HasAMap\",\n"
            + "  \"fields\": [{\n"
            + "    \"name\": \"optionalMap\",\n"
            + "    \"type\": [\"null\", {\n"
            + "      \"type\": \"map\",\n"
            + "      \"values\": \"string\"\n"
            + "    }]\n"
            + "  }, {\n"
            + "    \"name\": \"name\",\n"
            + "    \"type\": \"string\"\n"
            + "  }]\n"
            + "}";

    String readerSchemaStr = "{\n"
            + "  \"namespace\": \"org.apache.crunch.test\",\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"DoesntHaveAMap\",\n"
            + "  \"fields\": [{\n"
            + "    \"name\": \"name\",\n"
            + "    \"type\": \"string\"\n"
            + "  }]\n"
            + "}";

      Schema readerSchema = new Schema.Parser().parse(readerSchemaStr);

      Map<CharSequence, CharSequence> teamCities = new HashMap<>();
      teamCities.put(new Utf8("Ohio"), new Utf8("Cleveland"));

    Schema writerSchema = new Schema.Parser().parse(writerSchemaStr);
    GenericData.Record record = new GenericData.Record(writerSchema);
    record.put("optionalMap", teamCities);
    record.put("name", "generic record");

      Map<String, String> tableProps = new HashMap<>();
      tableProps.put("avro.schema.literal", writerSchema.toString());
      createAvroBackedTable(tableName, tableRootLocation, tableProps);
      File fileName = temporaryPath.getFile(tableName + ".avro");
      writeDatum(record, fileName);
      moveDatumsToHdfs(fileName, tableRootLocation, conf);

      Pipeline pipeline = new MRPipeline(HCatSourceITest.class, conf);
      Iterable<HCatRecord> players = pipeline.read(FromHCat.table(tableName)).materialize();

      int datumSize = 0;
      for (final HCatRecord hCatRecord : players) {
          datumSize++;

          DatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
          HCatDecoder decoder = new HCatDecoder(hCatRecord, writerSchema);
          GenericRecord readRecord = reader.read(null, decoder);
          assertThat(readRecord.get("name").toString(), is("generic record"));
          assertNull(readRecord.get("optionalMap"));
      }
  }

  @Test
  public void test_HCatToArrayOfSpecificRecords_Decoder()
      throws IOException, HiveException, TException, SerDeException {
    String tableName = testName.getMethodName();
    Path tableRootLocation = temporaryPath.getPath(tableName);

    Map<CharSequence, CharSequence> teamCities = new HashMap<>();
    teamCities.put(new Utf8("Ohio"), new Utf8("Cleveland"));
    teamCities.put(new Utf8("Missouri"), new Utf8("Kansas City"));
    teamCities.put(new Utf8("Washington"), new Utf8("Seattle"));
    List<CharSequence> teams = new SpecificData.Array<>(2, Player.getClassSchema().getField("previousTeams").schema());
    teams.add(new Utf8("Cleveland Indians"));
    teams.add(new Utf8("Kansas City Royals"));

    byte[] md5 = new byte[16];
    new Random().nextBytes(md5);
    Player player = Player.newBuilder().setBattingAvg(0.310).setName(new Utf8("Francisco Lindor")).setHomeruns(50)
        .setPosition(Position.CenterField).setPreviousTeams(teams).setTeamLocation(teamCities)
        .setTeamLocation(teamCities).setMd5(new md5(md5.clone())).build();

    Player player2 = Player.newBuilder().setBattingAvg(0.310).setName(new Utf8("Jason Kipnis")).setHomeruns(20)
        .setPosition(Position.SecondBase).setPreviousTeams(teams).setTeamLocation(teamCities)
        .setTeamLocation(teamCities).setMd5(new md5(md5.clone())).build();

    Players builtPlayers = Players.newBuilder().setPlayers(Arrays.asList(player, player2)).build();

    Map<String, String> tableProps = new HashMap<>();
    tableProps.put("avro.schema.literal", Players.getClassSchema().toString());

    createAvroBackedTable(tableName, tableRootLocation, tableProps);
    File fileName = temporaryPath.getFile("players_new.avro");
    writeDatum(builtPlayers, fileName);
    moveDatumsToHdfs(fileName, tableRootLocation, conf);

    Pipeline pipeline = new MRPipeline(HCatSourceITest.class, conf);
    Iterable<HCatRecord> players = pipeline.read(FromHCat.table(tableName)).materialize();

    HCatRecord hCatRecord = players.iterator().next();
    SpecificDatumReader<Players> reader = new SpecificDatumReader<>(Players.getClassSchema(), Players.getClassSchema());
    HCatDecoder decoder = new HCatDecoder(hCatRecord, Players.getClassSchema());
    Players read = reader.read(null, decoder);

    assertThat(read.getPlayers().size(), is(2));
    for (final Player player1 : read.getPlayers()) {
      if (player1.getName().equals(player.getName()))
        assertModel(player1, player, true);
      else
        assertModel(player1, player2, true);
    }
  }

  private org.apache.hadoop.hive.ql.metadata.Table createAvroBackedTable(String tableName, Path tableRootLocation,
      Map<String, String> tableProps) throws IOException, HiveException, TException {

    String serdeLib = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
    String inputFormat = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
    String outputFormat = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";

    return createTable(tableName, tableRootLocation, tableProps, new HashMap<String, String>(), serdeLib, inputFormat,
        outputFormat, new ArrayList<FieldSchema>());
  }

  private org.apache.hadoop.hive.ql.metadata.Table createTable(String tableName, Path tableRootLocation,
      Map<String, String> tableProps, Map<String, String> serDeParams, String serdeLib, String inputFormatClass,
      String outputFormatClass, List<FieldSchema> fields) throws IOException, HiveException, TException {

    org.apache.hadoop.hive.ql.metadata.Table tbl = new org.apache.hadoop.hive.ql.metadata.Table("default", tableName);
    tbl.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
    tbl.setTableType(TableType.EXTERNAL_TABLE);

    if (tableRootLocation != null)
      tbl.setDataLocation(tableRootLocation);

    tbl.setSerializationLib(serdeLib);

    if (StringUtils.isNotBlank(inputFormatClass))
      tbl.setInputFormatClass(inputFormatClass);

    if (StringUtils.isNotBlank(outputFormatClass))
      tbl.setOutputFormatClass(outputFormatClass);

    for (final Map.Entry<String, String> config : tableProps.entrySet()) {
      tbl.setProperty(config.getKey(), config.getValue());
    }

    for (final Map.Entry<String, String> config : serDeParams.entrySet()) {
      tbl.setSerdeParam(config.getKey(), config.getValue());
    }

    if (!fields.isEmpty())
      tbl.setFields(fields);

    client.createTable(tbl.getTTable());

    return tbl;
  }

  private <T extends IndexedRecord> void writeDatum(T datum, File location) throws IOException {
    // closed by the data file writer
    SpecificDatumWriter<T> writer = new SpecificDatumWriter<>();
    try (DataFileWriter<T> fileWriter = new DataFileWriter<>(writer)) {
      fileWriter.create(datum.getSchema(), location);
      fileWriter.append(datum);
    }
  }

  private void moveDatumsToHdfs(File src, Path dest, Configuration conf) throws IOException {
    FileSystem fs = dest.getFileSystem(conf);
    fs.mkdirs(dest);
    fs.copyFromLocalFile(new Path(src.toString()), dest);
  }
}
