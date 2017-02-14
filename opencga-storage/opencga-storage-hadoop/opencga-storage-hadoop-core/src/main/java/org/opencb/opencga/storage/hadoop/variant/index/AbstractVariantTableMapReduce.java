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

package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.collect.BiMap;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveResultToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Abstract variant table map reduce.
 *
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public abstract class AbstractVariantTableMapReduce extends AbstractHBaseMapReduce<ImmutableBytesWritable, Mutation> {
    public static final String COUNTER_GROUP_NAME = "OPENCGA.HBASE";
    public static final String SPECIFIC_PUT = "opencga.storage.hadoop.hbase.merge.use_specific_put";
    public static final String ARCHIVE_GET_BATCH_SIZE = "opencga.storage.hadoop.hbase.merge.archive.scan.batchsize";

    protected ArchiveResultToVariantConverter resultConverter;
    protected VariantMerger variantMerger;
    protected Set<String> currentIndexingSamples;
    protected Integer archiveBatchSize;


    protected ArchiveResultToVariantConverter getResultConverter() {
        return resultConverter;
    }

    protected VariantMerger getVariantMerger() {
        return variantMerger;
    }


    /**
     * Extracts file Ids from column names - ignoring _V columns.
     * @param value
     * @return Set of file IDs
     */
    private Set<Integer> extractFileIds(Result value) {
        return Arrays.stream(value.rawCells())
                .filter(c -> Bytes.equals(CellUtil.cloneFamily(c), getHelper().getColumnFamily()))
                .filter(c -> !Bytes.startsWith(CellUtil.cloneQualifier(c), GenomeHelper.VARIANT_COLUMN_B_PREFIX))
                .map(c -> Integer.parseInt(Bytes.toString(CellUtil.cloneQualifier(c))))
                .collect(Collectors.toSet());
    }

    protected List<Variant> parseCurrentVariantsRegion(List<Cell> variantCells, String chromosome) {
        List<VariantTableStudyRow> tableStudyRows = parseVariantStudyRowsFromArchive(variantCells, chromosome);
        HBaseToVariantConverter converter = getHbaseToVariantConverter();
        List<Variant> variants = new ArrayList<>(tableStudyRows.size());
        for (VariantTableStudyRow tableStudyRow : tableStudyRows) {
            variants.add(converter.convert(tableStudyRow));
        }
        return variants;
    }

    protected List<VariantTableStudyRow> parseVariantStudyRowsFromArchive(List<Cell> variantCells, String chr) {
        return variantCells.stream().flatMap(c -> {
            try {
                byte[] protoData = CellUtil.cloneValue(c);
                if (protoData != null && protoData.length > 0) {
                    List<VariantTableStudyRow> tableStudyRows =
                            parseVariantStudyRowsFromArchive(chr, VariantTableStudyRowsProto.parseFrom(protoData));
                    return tableStudyRows.stream();
                }
                return Stream.empty();
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
        }).collect(Collectors.toList());
    }

    protected List<VariantTableStudyRow> parseVariantStudyRowsFromArchive(String chr, VariantTableStudyRowsProto
            variantTableStudyRowsProto) {
        return variantTableStudyRowsProto.getRowsList().stream()
                            .map(v -> new VariantTableStudyRow(v, chr, getStudyConfiguration().getStudyId()))
                            .collect(Collectors.toList());
    }

    /**
     * Load (if available) current data, merge information and store new object in DB.
     *
     * @param context Context
     * @param analysisVar Analysis variants
     * @param rows Variant Table rows
     * @param newSampleIds Sample Ids currently processed
     */
    protected void updateOutputTable(Context context, Collection<Variant> analysisVar,
            List<VariantTableStudyRow> rows, Set<Integer> newSampleIds) {
        int studyId = getStudyConfiguration().getStudyId();
        BiMap<String, Integer> idMapping = getStudyConfiguration().getSampleIds();
        for (Variant variant : analysisVar) {
            VariantTableStudyRow row = updateOutputTable(context, studyId, idMapping, variant, newSampleIds);
            rows.add(row);
        }
    }

    protected VariantTableStudyRow updateOutputTable(Context context, int studyId, BiMap<String, Integer> idMapping,
                                                     Variant variant, Set<Integer> newSampleIds) {
        try {
            VariantTableStudyRow row = new VariantTableStudyRow(variant, studyId, idMapping);
            boolean specificPut = context.getConfiguration().getBoolean(SPECIFIC_PUT, true);
            Put put = null;
            if (specificPut && null != newSampleIds) {
                put = row.createSpecificPut(getHelper(), newSampleIds);
            } else {
                put = row.createPut(getHelper());
            }
            if (put != null) {
                context.write(new ImmutableBytesWritable(getHelper().getOutputTable()), put);
                context.getCounter(COUNTER_GROUP_NAME, "VARIANT_TABLE_ROW-put").increment(1);
            }
            return row;
        } catch (RuntimeException | InterruptedException | IOException e) {
            throw new IllegalStateException("Problems updating " + variant, e);
        }
    }

    protected void updateOutputTable(Context context, Collection<VariantTableStudyRow> variants) {

        for (VariantTableStudyRow variant : variants) {
            Put put = variant.createPut(getHelper());
            if (put != null) {
                try {
                    context.write(new ImmutableBytesWritable(getHelper().getOutputTable()), put);
                } catch (IOException | InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                context.getCounter(COUNTER_GROUP_NAME, "VARIANT_TABLE_ROW-put").increment(1);
            }
        }
    }

    protected void updateArchiveTable(byte[] rowKey, Context context, List<VariantTableStudyRow> tableStudyRows) {
        if (tableStudyRows.isEmpty()) {
            getLog().info("No new data - tableStudyRows emtpy");
            return;
        }
        getLog().info("Store variants: " + tableStudyRows.size());
        Put put = new Put(rowKey);
        for (VariantTableStudyRow row : tableStudyRows) {
            byte[] value = VariantTableStudyRow.toProto(Collections.singletonList(row), getTimestamp()).toByteArray();
            String column = GenomeHelper.getVariantcolumn(row);
            put.addColumn(getHelper().getColumnFamily(), Bytes.toBytes(column), value);
        }
        try {
            context.write(new ImmutableBytesWritable(getHelper().getIntputTable()), put);
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROW_PUT").increment(1);
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROWS_IN_PUT").increment(tableStudyRows.size());
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        this.archiveBatchSize = context.getConfiguration().getInt(ARCHIVE_GET_BATCH_SIZE, 500);

        // Load VCF meta data for columns
        int studyId = getStudyConfiguration().getStudyId();
        resultConverter = new ArchiveResultToVariantConverter(studyId, getHelper().getColumnFamily(), this.getStudyConfiguration());
        variantMerger = new VariantMerger(true);
        variantMerger.setStudyId(Integer.toString(studyId));

        String[] toIdxFileIds = context.getConfiguration().getStrings(AbstractVariantTableDriver.CONFIG_VARIANT_FILE_IDS, new String[0]);
        if (toIdxFileIds.length == 0) {
            throw new IllegalStateException(
                    "File IDs to be indexed not found in configuration: " + AbstractVariantTableDriver.CONFIG_VARIANT_FILE_IDS);
        }
        Set<String> toIndexSampleNames = new HashSet<>();
        Set<Integer> toIndexFileIdSet = Arrays.stream(toIdxFileIds).map(id -> Integer.valueOf(id)).collect(Collectors.toSet());
        BiMap<Integer, String> sampleIdToSampleName = StudyConfiguration.inverseMap(getStudyConfiguration().getSampleIds());
        for (BiMap.Entry<Integer, LinkedHashSet<Integer>> entry : getStudyConfiguration().getSamplesInFiles().entrySet()) {
            if (toIndexFileIdSet.contains(entry.getKey())) {
                entry.getValue().forEach(sid -> toIndexSampleNames.add(sampleIdToSampleName.get(sid)));
            }
        }
        getVariantMerger().setExpectedSamples(getIndexedSamples().keySet());
        // Add all samples which are currently being indexed.

        this.currentIndexingSamples = new HashSet<>(toIndexSampleNames);
        getVariantMerger().addExpectedSamples(toIndexSampleNames);
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        if (null != this.getHelper()) {
            this.getHelper().close();
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        getLog().info("Start mapping key: " + Bytes.toString(key.get()));
        startTime();
        if (value.isEmpty()) {
            context.getCounter(COUNTER_GROUP_NAME, "VCF_RESULT_EMPTY").increment(1);
            return; // TODO search backwards?
        }

        if (Bytes.equals(key.get(), getHelper().getMetaRowKey())) {
            return; // ignore metadata column
        }
        context.getCounter(COUNTER_GROUP_NAME, "VCF_BLOCK_READ").increment(1);


        // Calculate various positions
        byte[] currRowKey = key.get();
        String sliceKey = Bytes.toString(currRowKey);
        VariantTableHelper h = getHelper();
        String chr = h.extractChromosomeFromBlockId(sliceKey);
        Long sliceReg = h.extractSliceFromBlockId(sliceKey);
        long startPos = h.getStartPositionFromSlice(sliceReg);
        long nextStartPos = h.getStartPositionFromSlice(sliceReg + 1);

        Set<Integer> fileIds = extractFileIds(value);
        if (getLog().isDebugEnabled()) {
            getLog().debug("Results contain file IDs : " + StringUtils.join(fileIds, ','));
        }
        Set<Integer> sampleIds = new HashSet<>();
        for (Integer fid : fileIds) {
            LinkedHashSet<Integer> sids = getStudyConfiguration().getSamplesInFiles().get(fid);
            sampleIds.addAll(sids);
        }

        getLog().debug("Processing slice {}", sliceKey);


        VariantMapReduceContext ctx = new VariantMapReduceContext(currRowKey, context, value, fileIds,
                sampleIds, chr, startPos, nextStartPos);

        endTime("1 Prepare slice");

        /* *********************************** */
        /* ********* CALL concrete class ***** */
        doMap(ctx);
        /* *********************************** */

        // Clean up of this slice
        for (Entry<String, Long> entry : this.getTimes().entrySet()) {
            context.getCounter(COUNTER_GROUP_NAME, "VCF_TIMER_" + entry.getKey().replace(' ', '_')).increment(entry.getValue());
        }
        this.getTimes().clear();
        getLog().info("Finished mapping key: " + Bytes.toString(key.get()));
    }

    protected abstract void
    doMap(VariantMapReduceContext ctx) throws IOException, InterruptedException;

    public static class VariantMapReduceContext {
        public VariantMapReduceContext(byte[] currRowKey, Context context, Result value, Set<Integer> fileIds,
                                       Set<Integer> sampleIds, String chr, long startPos, long nextStartPos) {
            this.currRowKey = currRowKey;
            this.context = context;
            this.value = value;
            this.fileIds = fileIds;
            this.sampleIds = sampleIds;
            this.chr = chr;
            this.startPos = startPos;
            this.nextStartPos = nextStartPos;
        }

        protected final byte[] currRowKey;
        protected final Context context;
        protected final Result value;
        protected final Set<Integer> fileIds;
        protected final Set<Integer> sampleIds;
        private final String chr;
        protected final long startPos;
        protected final long nextStartPos;


        public byte[] getCurrRowKey() {
            return currRowKey;
        }

        public Context getContext() {
            return context;
        }

        public Result getValue() {
            return value;
        }

        public Set<Integer> getSampleIds() {
            return sampleIds;
        }

        public String getChromosome() {
            return chr;
        }

        public long getStartPos() {
            return startPos;
        }

        public long getNextStartPos() {
            return nextStartPos;
        }
    }
}
