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

package org.opencb.opencga.storage.hadoop.variant.converters.stats;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.avro.VariantHardyWeinbergStats;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.converters.Converter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 07/07/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsToHBaseConverter extends AbstractPhoenixConverter implements Converter<VariantStatsWrapper, Put> {

    private final GenomeHelper genomeHelper;
    private final StudyConfiguration studyConfiguration;
    private final int studyId;
    private final Logger logger = LoggerFactory.getLogger(VariantStatsToHBaseConverter.class);

    public VariantStatsToHBaseConverter(GenomeHelper genomeHelper, StudyConfiguration studyConfiguration) {
        super(genomeHelper.getColumnFamily());
        this.genomeHelper = genomeHelper;
        this.studyConfiguration = studyConfiguration;
        this.studyId = studyConfiguration.getStudyId();
    }

    @Override
    public Put convert(VariantStatsWrapper variantStatsWrapper) {
        if (variantStatsWrapper.getCohortStats() == null || variantStatsWrapper.getCohortStats().isEmpty()) {
            return null;
        }

        VariantStats firstStats = variantStatsWrapper.getCohortStats().entrySet().iterator().next().getValue();
        byte[] row = genomeHelper.generateVariantRowKey(
                variantStatsWrapper.getChromosome(), variantStatsWrapper.getPosition(),
                firstStats.getRefAllele(), firstStats.getAltAllele());
        Put put = new Put(row);
        for (Map.Entry<String, VariantStats> entry : variantStatsWrapper.getCohortStats().entrySet()) {
            Integer cohortId = studyConfiguration.getCohortIds().get(entry.getKey());
            Column mafColumn = VariantPhoenixHelper.getMafColumn(studyId, cohortId);
            Column mgfColumn = VariantPhoenixHelper.getMgfColumn(studyId, cohortId);
            Column statsColumn = VariantPhoenixHelper.getStatsColumn(studyId, cohortId);

            VariantStats stats = entry.getValue();
            add(put, mafColumn, stats.getMaf());
            add(put, mgfColumn, stats.getMgf());

            VariantProto.VariantStats.Builder builder = VariantProto.VariantStats.newBuilder()
                    .setAltAlleleFreq(stats.getAltAlleleFreq())
                    .setAltAlleleCount(stats.getAltAlleleCount())
                    .setRefAlleleFreq(stats.getRefAlleleFreq())
                    .setRefAlleleCount(stats.getRefAlleleCount())
                    .setMissingAlleles(stats.getMissingAlleles())
                    .setMissingGenotypes(stats.getMissingGenotypes());

            if (stats.getMafAllele() != null) {
                builder.setMaf(stats.getMaf())
                        .setMafAllele(stats.getMafAllele());
            }

            if (stats.getMgfGenotype() != null) {
                builder.setMgf(stats.getMgf())
                        .setMgfGenotype(stats.getMgfGenotype());
            }

            if (stats.getGenotypesCount() != null) {
                Map<String, Integer> map = new HashMap<>(stats.getGenotypesCount().size());
                stats.getGenotypesCount().forEach((genotype, count) -> map.put(genotype.toString(), count));
                builder.putAllGenotypesCount(map);
            }

            if (stats.getGenotypesFreq() != null) {
                Map<String, Float> map = new HashMap<>(stats.getGenotypesFreq().size());
                stats.getGenotypesFreq().forEach((genotype, freq) -> map.put(genotype.toString(), freq));
                builder.putAllGenotypesFreq(map);
            }

            if (stats.getHw() != null) {
                builder.setHw(build(stats.getHw()));
            }
            add(put, statsColumn, builder.build().toByteArray());
        }
        return put;
    }

    private VariantProto.VariantHardyWeinbergStats.Builder build(VariantHardyWeinbergStats hw) {
        VariantProto.VariantHardyWeinbergStats.Builder builder = VariantProto.VariantHardyWeinbergStats.newBuilder();
        builder.setChi2(ObjectUtils.firstNonNull(hw.getChi2(), -1f));
        builder.setPValue(ObjectUtils.firstNonNull(hw.getPValue(), -1f));

        builder.setN(ObjectUtils.firstNonNull(hw.getN(), -1));

        builder.setNAa00(ObjectUtils.firstNonNull(hw.getNAa00(), -1));
        builder.setNAa10(ObjectUtils.firstNonNull(hw.getNAa10(), -1));
        builder.setNAA11(ObjectUtils.firstNonNull(hw.getNAA11(), -1));

        builder.setEAa00(ObjectUtils.firstNonNull(hw.getEAa00(), -1f));
        builder.setEAa10(ObjectUtils.firstNonNull(hw.getEAa10(), -1f));
        builder.setEAA11(ObjectUtils.firstNonNull(hw.getEAA11(), -1f));

        builder.setP(ObjectUtils.firstNonNull(hw.getP(), -1f));
        builder.setQ(ObjectUtils.firstNonNull(hw.getQ(), -1f));
        return builder;
    }

}
