/*
 * Copyright 2015-2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDriver;
import org.opencb.opencga.storage.hadoop.utils.HBaseLock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created on 12/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseStudyConfigurationManager extends StudyConfigurationManager {

    private final byte[] studiesRow;
    private final byte[] studiesSummaryColumn;

    private final Configuration configuration;
    private final ObjectMap options;
    private final GenomeHelper genomeHelper;
    private final ObjectMapper objectMapper;
    private final String tableName;
    private final HBaseLock lock;

    public HBaseStudyConfigurationManager(GenomeHelper helper, String tableName, Configuration configuration, ObjectMap options)
            throws IOException {
        super(options);
        this.configuration = Objects.requireNonNull(configuration);
        this.tableName = Objects.requireNonNull(tableName);
        this.options = options;
        this.genomeHelper = helper;
        this.objectMapper = new ObjectMapper();
        this.studiesRow = genomeHelper.generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);
        this.studiesSummaryColumn = genomeHelper.generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);
        lock = new HBaseLock(getHBaseManager(), this.tableName, genomeHelper.getColumnFamily(), studiesRow);
    }

    public HBaseStudyConfigurationManager(String tableName, Configuration configuration, ObjectMap options)
            throws IOException {
        this(new GenomeHelper(configuration), tableName, configuration, options);
    }

    @Override
    protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        logger.debug("Get StudyConfiguration " + studyId + " from DB " + tableName);
        return internalGetStudyConfiguration(getStudies(options).inverse().get(studyId), timeStamp, options);
    }

    @Override
    public long lockStudy(int studyId, long lockDuration, long timeout) throws InterruptedException, TimeoutException {
        try {
            VariantTableDriver.createVariantTableIfNeeded(genomeHelper, tableName, getConnection());
            return lock.lock(Bytes.toBytes(studyId + "_LOCK"), lockDuration, timeout);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void unLockStudy(int studyId, long lockToken) {
        try {
            lock.unlock(Bytes.toBytes(studyId + "_LOCK"), lockToken);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public GenomeHelper getGenomeHelper() {
        return this.genomeHelper;
    }

    protected HBaseManager getHBaseManager() {
        return this.getGenomeHelper().getHBaseManager();
    }

    @Override
    protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {
        long startTime = System.currentTimeMillis();
        String error = null;
        List<StudyConfiguration> studyConfigurationList = Collections.emptyList();
        logger.debug("Get StudyConfiguration {} from DB {}", studyName, tableName);
        if (StringUtils.isEmpty(studyName)) {
            return new QueryResult<>("", (int) (System.currentTimeMillis() - startTime),
                    studyConfigurationList.size(), studyConfigurationList.size(), "", "", studyConfigurationList);
        }
        Get get = new Get(studiesRow);
        byte[] columnQualifier = Bytes.toBytes(studyName);
        get.addColumn(genomeHelper.getColumnFamily(), columnQualifier);
        if (timeStamp != null) {
            try {
                get.setTimeRange(timeStamp + 1, Long.MAX_VALUE);
            } catch (IOException e) {
                //This should not happen ever.
                throw new IllegalArgumentException(e);
            }
        }

        try {
            if (getHBaseManager().act(getConnection(), tableName, (table, admin) -> admin.tableExists(table.getName()))) {
                studyConfigurationList = getHBaseManager().act(getConnection(), tableName, table -> {
                    Result result = table.get(get);
                    if (result.isEmpty()) {
                        return Collections.emptyList();
                    } else {
                        byte[] value = result.getValue(genomeHelper.getColumnFamily(), columnQualifier);
                        StudyConfiguration studyConfiguration = toStudyConfiguration(value);
                        return Collections.singletonList(studyConfiguration);
                    }
                });
            }
        } catch (IOException e) {
            throw new IllegalStateException("Problem checking Table " + tableName, e);
        }
        return new QueryResult<>("", (int) (System.currentTimeMillis() - startTime),
                studyConfigurationList.size(), studyConfigurationList.size(), "", error, studyConfigurationList);
    }

    @Override
    protected QueryResult internalUpdateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        long startTime = System.currentTimeMillis();
        String error = "";
        logger.info("Update StudyConfiguration {}", studyConfiguration.getStudyName());
        updateStudiesSummary(studyConfiguration.getStudyName(), studyConfiguration.getStudyId(), options);
        byte[] columnQualifier = Bytes.toBytes(studyConfiguration.getStudyName());

        studyConfiguration.getHeaders().clear(); // REMOVE: stored in Archive table

        try {
            getHBaseManager().act(tableName, table -> {
                byte[] bytes = toBytes(studyConfiguration);
                Put put = new Put(studiesRow);
                put.addColumn(genomeHelper.getColumnFamily(), columnQualifier, studyConfiguration.getTimeStamp(), bytes);
                table.put(put);
            });
        } catch (IOException e) {
            e.printStackTrace();
            error = e.getMessage();
        }

        return new QueryResult<>("", (int) (System.currentTimeMillis() - startTime), 0, 0, "", error, Collections.emptyList());
    }

    protected byte[] toBytes(StudyConfiguration studyConfiguration) throws JsonProcessingException {
        byte[] data = objectMapper.writeValueAsBytes(studyConfiguration);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(bout)) {
            out.write(data);
            out.flush();
        } catch (IOException e) {
            throw new IllegalStateException("Problems compressing study configuration ...");
        }
        return bout.toByteArray();
    }

    public static boolean isGzipped(byte[] data) {
        return data[0] == (byte) 0x1f && data[1] == (byte) 0x8b;
    }

    protected StudyConfiguration toStudyConfiguration(byte[] data) throws IOException {
        byte[] value = data;
        if (isGzipped(data)) { // backwards compatibility
            value = IOUtils.toByteArray(
                    new GZIPInputStream(
                            new ByteArrayInputStream(data)));
        }
        return objectMapper.readValue(value, StudyConfiguration.class);
    }

    @Override
    public BiMap<String, Integer> getStudies(QueryOptions options) {
        Get get = new Get(studiesRow);
        get.addColumn(genomeHelper.getColumnFamily(), studiesSummaryColumn);
        try {
            if (!getHBaseManager().act(tableName, (table, admin) -> admin.tableExists(table.getName()))) {
                logger.debug("Get StudyConfiguration summary TABLE_NO_EXISTS");
                return HashBiMap.create();
            }
            return getHBaseManager().act(tableName, table -> {
                Result result = table.get(get);
                if (result.isEmpty()) {
                    logger.debug("Get StudyConfiguration summary EMPTY");
                    return HashBiMap.create();
                } else {
                    byte[] value = result.getValue(genomeHelper.getColumnFamily(), studiesSummaryColumn);
                    Map<String, Integer> map = objectMapper.readValue(value, Map.class);
                    logger.debug("Get StudyConfiguration summary {}", map);

                    return HashBiMap.create(map);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("Get StudyConfiguration summary ERROR");
            return HashBiMap.create();
        }
    }

    private void updateStudiesSummary(String study, Integer studyId, QueryOptions options) {
        BiMap<String, Integer> studiesSummary = getStudies(options);
        if (study.isEmpty()) {
            throw new IllegalStateException("Can't save an study with empty StudyName");
        }
        if (studiesSummary.getOrDefault(study, Integer.MIN_VALUE).equals(studyId)) {
            //Nothing to update
            return;
        } else {
            studiesSummary.put(study, studyId);
            updateStudiesSummary(studiesSummary, options);
        }
    }

    private void updateStudiesSummary(BiMap<String, Integer> studies, QueryOptions options) {
        try {
            VariantTableDriver.createVariantTableIfNeeded(genomeHelper, tableName, getConnection());
            try (Table table = getConnection().getTable(TableName.valueOf(tableName))) {
                byte[] bytes = objectMapper.writeValueAsBytes(studies);
                Put put = new Put(studiesRow);
                put.addColumn(genomeHelper.getColumnFamily(), studiesSummaryColumn, bytes);
                table.put(put);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Connection getConnection() throws IOException {
        return this.getHBaseManager().getConnection();
    }

    @Override
    public void close() throws IOException {
        try {
            this.getHBaseManager().close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
