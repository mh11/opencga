package org.opencb.opencga.storage.hadoop.variant.stats;

import com.google.common.collect.BiMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsCalculator;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.VariantStatsToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 07/12/2016.
 */
public class AnalysisStatsMapper extends AbstractHBaseMapReduce<ImmutableBytesWritable, Put> {

    private VariantStatisticsCalculator variantStatisticsCalculator;
    private String studyId;
    private byte[] studiesRow;
    private Map<String, Set<String>> samples;
    private VariantStatsToHBaseConverter variantStatsToHBaseConverter;
    private long prefEnd = -1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.getHbaseToVariantConverter().setSimpleGenotypes(true);
        studiesRow = getHelper().generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);
        variantStatisticsCalculator = new VariantStatisticsCalculator(true);
        this.variantStatisticsCalculator.setAggregationType(VariantSource.Aggregation.NONE, null);
        this.studyId = Integer.valueOf(this.getStudyConfiguration().getStudyId()).toString();
        BiMap<Integer, String> sampleIds = getStudyConfiguration().getSampleIds().inverse();
        variantStatsToHBaseConverter = new VariantStatsToHBaseConverter(this.getHelper(), this.getStudyConfiguration());
        // map from cohort Id to <cohort name, <sample names>>
        this.samples = this.getStudyConfiguration().getCohortIds().entrySet().stream()
                .map(e -> new MutablePair<>(e.getKey(), this.getStudyConfiguration().getCohorts().get(e.getValue())))
                .map(p -> new MutablePair<>(p.getKey(),
                        p.getValue().stream().map(i -> sampleIds.get(i)).collect(Collectors.toSet())))
                .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
        this.samples.forEach((k, v) -> getLog().info("Calculate {} stats for cohort {} with {}", studyId, k, StringUtils.join(v, ",")));
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        boolean done = false;
        if (!Bytes.startsWith(value.getRow(), this.studiesRow)) { // ignore _METADATA row
            try {
                long start = System.nanoTime();
                if (prefEnd > 0) {
                    context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, "nano-next").increment(start - prefEnd);
                }
                getLog().info("Convert Variant ...");
                Variant variant = convert(value);
                long convertTime = System.nanoTime();
                context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, "variants-converted").increment(1);
                getLog().info("Calculate Stats ...");
                List<VariantStatsWrapper> annotations = calculateStats(variant);
                long calc = System.nanoTime();
                for (VariantStatsWrapper annotation : annotations) {
                    Put convert = convertToPut(annotation);
                    if (null != convert) {
                        submitStats(key, context, convert);
                        done = true;
                        context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, "stats.put").increment(1);
                    }
                }
                long submit = System.nanoTime();
                if (done) {
                    context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, "variants").increment(1);
                    context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, "nano-convert").increment(convertTime - start);
                    context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, "nano-calc").increment(calc - convertTime);
                    context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, "nano-submit").increment(submit - calc);
                }
            } catch (IllegalStateException e) {
                throw new IllegalStateException("Problem with row [hex:" + Bytes.toHex(key.copyBytes()) + "]", e);
            }
        }
        prefEnd = System.nanoTime();
    }

    protected void submitStats(ImmutableBytesWritable key, Context context, Put convert) throws IOException,
            InterruptedException {
        context.write(key, convert);
    }

    protected Put convertToPut(VariantStatsWrapper annotation) {
        return this.variantStatsToHBaseConverter.convert(annotation);
    }

    protected List<VariantStatsWrapper> calculateStats(Variant variant) {
        return this.variantStatisticsCalculator.calculateBatch(
                Collections.singletonList(variant), this.studyId, "notused", this.samples);
    }

    protected Variant convert(Result value) {
        return this.getHbaseToVariantConverter().convert(value);
    }
}
