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

package org.opencb.opencga.storage.mongodb.variant;

import org.apache.log4j.Level;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.core.common.MemoryUsageMonitor;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.FileStudyConfigurationManager;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.db.VariantAnnotationDBWriter;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.opencb.opencga.storage.mongodb.metadata.MongoDBStudyConfigurationManager;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.io.db.VariantMongoDBAnnotationDBWriter;
import org.opencb.opencga.storage.mongodb.variant.load.MongoVariantImporter;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.*;

import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.*;

/**
 * Created by imedina on 13/08/14.
 */
public class MongoDBVariantStorageEngine extends VariantStorageEngine {

    /*
     * This field defaultValue must be the same that the one at storage-configuration.yml
     */
    public static final String STORAGE_ENGINE_ID = "mongodb";

    // Connection to MongoDB.
    private MongoDataStoreManager mongoDataStoreManager = null;

    public enum MongoDBVariantOptions {
        COLLECTION_VARIANTS("collection.variants", "variants"),
        COLLECTION_FILES("collection.files", "files"),
        COLLECTION_STUDIES("collection.studies",  "studies"),
        COLLECTION_STAGE("collection.stage",  "stage"),
        BULK_SIZE("bulkSize",  100),
        DEFAULT_GENOTYPE("defaultGenotype", Arrays.asList("0/0", "0|0")),
        ALREADY_LOADED_VARIANTS("alreadyLoadedVariants", 0),
        STAGE("stage", false),
        STAGE_RESUME("stage.resume", false),
        STAGE_PARALLEL_WRITE("stage.parallel.write", false),
        MERGE("merge", false),
        MERGE_SKIP("merge.skip", false), // Internal use only
        MERGE_RESUME("merge.resume", false),
        MERGE_PARALLEL_WRITE("merge.parallel.write", false);

        private final String key;
        private final Object value;

        MongoDBVariantOptions(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public static boolean isResumeStage(ObjectMap options) {
            return options.getBoolean(Options.RESUME.key(), Options.RESUME.defaultValue())
                    || options.getBoolean(STAGE_RESUME.key(), false);
        }

        public static boolean isResumeMerge(ObjectMap options) {
            return options.getBoolean(Options.RESUME.key(), Options.RESUME.defaultValue())
                    || options.getBoolean(MERGE_RESUME.key(), false);
        }

        public String key() {
            return key;
        }

        @SuppressWarnings("unchecked")
        public <T> T defaultValue() {
            return (T) value;
        }
    }

    public MongoDBVariantStorageEngine() {
        //Disable MongoDB useless logging
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.cluster").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.connection").setLevel(Level.WARN);
    }

    @Override
    public void testConnection() throws StorageEngineException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        String dbName = options.getString(VariantStorageEngine.Options.DB_NAME.key());
        MongoCredentials credentials = getMongoCredentials(dbName);

        if (!credentials.check()) {
            logger.error("Connection to database '{}' failed", dbName);
            throw new StorageEngineException("Database connection test failed");
        }
    }

    @Override
    protected VariantImporter newVariantImporter(VariantDBAdaptor dbAdaptor) {
        VariantMongoDBAdaptor mongoDBAdaptor = (VariantMongoDBAdaptor) dbAdaptor;
        return new MongoVariantImporter(mongoDBAdaptor);
    }

    @Override
    public MongoDBVariantStoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException {
        VariantMongoDBAdaptor dbAdaptor = connected ? getDBAdaptor(null) : null;
        return new MongoDBVariantStoragePipeline(configuration, STORAGE_ENGINE_ID, dbAdaptor);
    }

    @Override
    protected VariantAnnotationManager newVariantAnnotationManager(VariantAnnotator annotator, VariantDBAdaptor dbAdaptor) {
        VariantMongoDBAdaptor mongoDBAdaptor = (VariantMongoDBAdaptor) dbAdaptor;
        return new DefaultVariantAnnotationManager(annotator, dbAdaptor) {
            @Override
            protected VariantAnnotationDBWriter newVariantAnnotationDBWriter(VariantDBAdaptor dbAdaptor, QueryOptions options) {
                return new VariantMongoDBAnnotationDBWriter(options, mongoDBAdaptor);
            }
        };
    }

    @Override
    public void dropFile(String study, int fileId) throws StorageEngineException {
        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        getDBAdaptor().deleteFile(study, Integer.toString(fileId), new QueryOptions(options));
    }

    @Override
    public void dropStudy(String studyName) throws StorageEngineException {
        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        getDBAdaptor().deleteStudy(studyName, new QueryOptions(options));
    }

    @Override
    public VariantMongoDBAdaptor getDBAdaptor() throws StorageEngineException {
        return getDBAdaptor(null);
    }

    @Override
    public List<StoragePipelineResult> index(List<URI> inputFiles, URI outdirUri, boolean doExtract, boolean doTransform, boolean doLoad)
            throws StorageEngineException {

        Map<URI, MongoDBVariantStoragePipeline> storageETLMap = new LinkedHashMap<>();
        Map<URI, StoragePipelineResult> resultsMap = new LinkedHashMap<>();
        LinkedList<StoragePipelineResult> results = new LinkedList<>();

        MemoryUsageMonitor monitor = new MemoryUsageMonitor();
        monitor.setDelay(5000);
//        monitor.start();
        try {
            for (URI inputFile : inputFiles) {
                StoragePipelineResult storagePipelineResult = new StoragePipelineResult(inputFile);
                MongoDBVariantStoragePipeline storageETL = newStoragePipeline(doLoad);
                storageETL.getOptions().append(VariantStorageEngine.Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(), true);
                storageETLMap.put(inputFile, storageETL);
                resultsMap.put(inputFile, storagePipelineResult);
                results.add(storagePipelineResult);
            }


            if (doExtract) {
                for (Map.Entry<URI, MongoDBVariantStoragePipeline> entry : storageETLMap.entrySet()) {
                    URI uri = entry.getValue().extract(entry.getKey(), outdirUri);
                    resultsMap.get(entry.getKey()).setExtractResult(uri);
                }
            }

            if (doTransform) {
                for (Map.Entry<URI, MongoDBVariantStoragePipeline> entry : storageETLMap.entrySet()) {
                    StoragePipelineResult etlResult = resultsMap.get(entry.getKey());
                    URI input = etlResult.getExtractResult() == null ? entry.getKey() : etlResult.getExtractResult();
                    transformFile(entry.getValue(), etlResult, results, input, outdirUri);
                }
            }

            boolean doStage = getOptions().getBoolean(STAGE.key());
            boolean doMerge = getOptions().getBoolean(MERGE.key());
            if (!doStage && !doMerge) {
                doStage = true;
                doMerge = true;
            }

            if (doLoad) {
                int batchLoad = getOptions().getInt(Options.MERGE_BATCH_SIZE.key(), Options.MERGE_BATCH_SIZE.defaultValue());
                // Files to merge
                List<Integer> filesToMerge = new ArrayList<>(batchLoad);
                List<StoragePipelineResult> resultsToMerge = new ArrayList<>(batchLoad);
                List<Integer> mergedFiles = new ArrayList<>();
                String study = null;

                Iterator<Map.Entry<URI, MongoDBVariantStoragePipeline>> iterator = storageETLMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<URI, MongoDBVariantStoragePipeline> entry = iterator.next();
                    StoragePipelineResult etlResult = resultsMap.get(entry.getKey());
                    URI input = etlResult.getPostTransformResult() == null ? entry.getKey() : etlResult.getPostTransformResult();
                    MongoDBVariantStoragePipeline storageETL = entry.getValue();

                    if (doStage) {
                        storageETL.getOptions().put(STAGE.key(), true);
                        storageETL.getOptions().put(MERGE.key(), false);
                        loadFile(storageETL, etlResult, results, input, outdirUri);
                        etlResult.setLoadExecuted(false);
                        etlResult.getLoadStats().put(STAGE.key(), true);
                    }

                    if (doMerge) {
                        filesToMerge.add(storageETL.getOptions().getInt(Options.FILE_ID.key()));
                        study = storageETL.getStudyConfiguration().getStudyName();
                        resultsToMerge.add(etlResult);
                        if (filesToMerge.size() == batchLoad || !iterator.hasNext()) {
                            long millis = System.currentTimeMillis();
                            try {
                                storageETL.getOptions().put(MERGE.key(), true);
                                storageETL.getOptions().put(Options.FILE_ID.key(), new ArrayList<>(filesToMerge));
                                storageETL.merge(filesToMerge);
                                storageETL.postLoad(input, outdirUri);
                            } catch (Exception e) {
                                for (StoragePipelineResult storagePipelineResult : resultsToMerge) {
                                    storagePipelineResult.setLoadError(e);
                                }
                                throw new StoragePipelineException("Exception executing merge.", e, results);
                            } finally {
                                long mergeTime = System.currentTimeMillis() - millis;
                                for (StoragePipelineResult storagePipelineResult : resultsToMerge) {
                                    storagePipelineResult.setLoadTimeMillis(storagePipelineResult.getLoadTimeMillis() + mergeTime);
                                    for (Map.Entry<String, Object> statsEntry : storageETL.getLoadStats().entrySet()) {
                                        storagePipelineResult.getLoadStats().putIfAbsent(statsEntry.getKey(), statsEntry.getValue());
                                    }
                                    storagePipelineResult.setLoadExecuted(true);
                                }
                                mergedFiles.addAll(filesToMerge);
                                filesToMerge.clear();
                                resultsToMerge.clear();
                            }
                        }
                    }
                }
                if (doMerge) {
                    annotateLoadedFiles(outdirUri, inputFiles, results, getOptions());
                    calculateStatsForLoadedFiles(outdirUri, inputFiles, results, getOptions());
                }
            }

        } finally {
//            monitor.interrupt();
            for (StoragePipeline storagePipeline : storageETLMap.values()) {
                storagePipeline.close();
            }
        }

        return results;
    }

    @Override
    public VariantMongoDBAdaptor getDBAdaptor(String dbName) throws StorageEngineException {
        MongoCredentials credentials = getMongoCredentials(dbName);
        VariantMongoDBAdaptor variantMongoDBAdaptor;
        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        if (dbName != null && !dbName.isEmpty()) {
            options.append(VariantStorageEngine.Options.DB_NAME.key(), dbName);
        }

        String variantsCollection = options.getString(COLLECTION_VARIANTS.key(), COLLECTION_VARIANTS.defaultValue());
        String filesCollection = options.getString(COLLECTION_FILES.key(), COLLECTION_FILES.defaultValue());
        MongoDataStoreManager mongoDataStoreManager = getMongoDataStoreManager();
        try {
            StudyConfigurationManager studyConfigurationManager = getStudyConfigurationManager(options);
            variantMongoDBAdaptor = new VariantMongoDBAdaptor(mongoDataStoreManager, credentials, variantsCollection, filesCollection,
                    studyConfigurationManager, configuration);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }

        logger.debug("getting DBAdaptor to db: {}", credentials.getMongoDbName());
        return variantMongoDBAdaptor;
    }

    MongoCredentials getMongoCredentials(String dbName) {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();

        // If no database name is provided, read from the configuration file
        if (dbName == null || dbName.isEmpty()) {
            dbName = options.getString(VariantStorageEngine.Options.DB_NAME.key(), VariantStorageEngine.Options.DB_NAME.defaultValue());
        }

        DatabaseCredentials database = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getDatabase();

        try {
            return new MongoCredentials(database, dbName);
        } catch (IllegalOpenCGACredentialsException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected StudyConfigurationManager buildStudyConfigurationManager(ObjectMap options) throws StorageEngineException {
        if (options != null && !options.getString(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, "").isEmpty()) {
            return super.buildStudyConfigurationManager(options);
        } else {
            String dbName = options == null ? null : options.getString(VariantStorageEngine.Options.DB_NAME.key());
            String collectionName = options == null ? null : options.getString(COLLECTION_STUDIES.key(), COLLECTION_STUDIES.defaultValue());
            try {
                return new MongoDBStudyConfigurationManager(getMongoDataStoreManager(), getMongoCredentials(dbName), collectionName);
//                return getDBAdaptor(dbName).getStudyConfigurationManager();
            } catch (UnknownHostException e) {
                throw new StorageEngineException("Unable to build MongoStorageConfigurationManager", e);
            }
        }
    }

    private synchronized MongoDataStoreManager getMongoDataStoreManager() {
        if (mongoDataStoreManager == null) {
            mongoDataStoreManager = new MongoDataStoreManager(getMongoCredentials(null).getDataStoreServerAddresses());
        }
        return mongoDataStoreManager;
    }

    public synchronized void close() {
        if (mongoDataStoreManager != null) {
            mongoDataStoreManager.close();
            mongoDataStoreManager = null;
        }
    }

}
