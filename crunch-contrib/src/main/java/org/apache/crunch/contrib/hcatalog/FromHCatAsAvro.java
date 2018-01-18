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

import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.contrib.hcatalog.avro.HCatToAvroFn;
import org.apache.crunch.io.hcatalog.FromHCat;
import org.apache.crunch.io.hcatalog.HCatSourceTarget;
import org.apache.crunch.types.avro.Avros;
import org.apache.hive.hcatalog.data.HCatRecord;

import javax.annotation.Nullable;

/**
 * @author Stephen Durfey
 */
public class FromHCatAsAvro {

    /**
     * Reads a {@link PCollection} of the specified {@code avroClazz} from the
     * requested hive {@code table}. By default reads from the 'default' hive
     * database.
     *
     * The provided {@code avroClazz} MUST be compatible with the
     * {@link HCatRecord} representation of the data (e.g. the table being read
     * from should be an avro backed table). If the {@code avroClazz} is not
     * compatible then the outcome is undefined.
     *
     * @param pipeline
     *          the pipeline to use for reading
     * @param table
     *          the hive table to read from
     * @param avroClazz
     *          the avro class to convert into
     * @param <T>
     *          the type, must extend {@link SpecificRecord}
     * @return a {@link PCollection} of the specified {@code avroClazz}
     * @throws IllegalArgumentException
     *           if table is null or empty
     */
    public static <T extends SpecificRecord> PCollection<T> table(Pipeline pipeline, String table,
                                                                  Class<T> avroClazz) {
        return table(pipeline, null, table, avroClazz);
    }

    /**
     * Reads a {@link PCollection} of the specified {@code avroClazz} from the
     * requested hive {@code table} and {@code database}.
     *
     * The provided {@code avroClazz} MUST be compatible with the
     * {@link HCatRecord} representation of the data (e.g. the table being read
     * from should be an avro backed table). If the {@code avroClazz} is not
     * compatible then the outcome is undefined.
     *
     * @param pipeline
     *          the pipeline to use for reading
     * @param database
     *          the database to use to find the table
     * @param table
     *          the hive table to read from
     * @param avroClazz
     *          the avro class to convert into
     * @param <T>
     *          the type, must extend {@link SpecificRecord}
     * @return a {@link PCollection} of the specified {@code avroClazz}
     * @throws IllegalArgumentException
     *           if table is null or empty
     */
    public static <T extends SpecificRecord> PCollection<T> table(Pipeline pipeline, @Nullable String database, String table,
                                                                  Class<T> avroClazz) {
        return table(pipeline, database, table, null, avroClazz);
    }

    /**
     * Reads a {@link PCollection} of the specified {@code avroClazz} from the
     * requested hive {@code table} and {@code database}. The returned data will
     * be limited by the {@code filter}. If the {@code filter} is empty, then no
     * restrictions will be placed on what is returned.
     *
     * The provided {@code avroClazz} MUST be compatible with the
     * {@link HCatRecord} representation of the data (e.g. the table being read
     * from should be an avro backed table). If the {@code avroClazz} is not
     * compatible then the outcome is undefined.
     *
     * @param pipeline
     *          the pipeline to use for reading
     * @param database
     *          the database to use to find the table
     * @param table
     *          the hive table to read from
     * @param filter
     *          the filter to limit the data (or not, if null)
     * @param avroClazz
     *          the avro class to convert into
     * @param <T>
     *          the type, must extend {@link SpecificRecord}
     * @return a {@link PCollection} of the specified {@code avroClazz}
     * @throws IllegalArgumentException
     *           if table is null or empty
     */
    public static <T extends SpecificRecord> PCollection<T> table(Pipeline pipeline, @Nullable String database,
            String table, @Nullable String filter, Class<T> avroClazz) {
        return pipeline.read(new HCatSourceTarget(database, table, filter))
                .parallelDo(new HCatToAvroFn<>(avroClazz),
                Avros.records(avroClazz));
    }
}
