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
package org.apache.crunch.io.impl;


import org.apache.commons.io.FileUtils;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.hasItem;

public class FileTargetImplTest {

  @Rule
  public TemporaryFolder TMP = new TemporaryFolder();

  @Test
  public void testHandleOutputsMovesFilesToDestination() throws Exception {
    java.nio.file.Path testWorkingPath = TMP.newFolder().toPath();
    java.nio.file.Path testDestinationPath = TMP.newFolder().toPath();
    FileTargetImpl fileTarget = new FileTargetImpl(
        new Path(testDestinationPath.toAbsolutePath().toString()),
        SequenceFileOutputFormat.class,
        SequentialFileNamingScheme.getInstance());

    Map<String, String> partToContent = new HashMap<>();
    partToContent.put("part-m-00000", "test1");
    partToContent.put("part-m-00001", "test2");
    Collection<String> expectedContent = partToContent.values();

    for(Map.Entry<String, String> entry: partToContent.entrySet()){
      File part = new File(testWorkingPath.toAbsolutePath().toString(), entry.getKey());
      FileUtils.writeStringToFile(part, entry.getValue());
    }
    fileTarget.handleOutputs(new Configuration(),
        new Path(testWorkingPath.toAbsolutePath().toString()),
        -1);

    Set<String> fileContents = new HashSet<>();
    for (String fileName : partToContent.keySet()) {
      fileContents.add(FileUtils.readFileToString(testDestinationPath.resolve(fileName).toFile()));
    }

    assertThat(expectedContent.size(), is(fileContents.size()));
    for(String content: expectedContent){
      assertThat(fileContents, hasItem(content));
    }
  }
}