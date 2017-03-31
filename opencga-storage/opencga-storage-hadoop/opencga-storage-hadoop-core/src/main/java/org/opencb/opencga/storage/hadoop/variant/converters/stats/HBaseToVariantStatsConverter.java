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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.avro.VariantHardyWeinbergStats;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Created on 07/07/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToVariantStatsConverter extends AbstractPhoenixConverter {

    private final Logger logger = LoggerFactory.getLogger(HBaseToVariantStatsConverter.class);

    public HBaseToVariantStatsConverter(GenomeHelper genomeHelper) {
        super(genomeHelper.getColumnFamily());
    }
//
//    public static Converter<Result, List<VariantStats>> fromHbase(GenomeHelper genomeHelper) {
//        HBaseToVariantStatsConverter converter = new HBaseToVariantStatsConverter(genomeHelper);
//        return converter::convert;
//    }
//
//    public static Converter<ResultSet, List<VariantStats>> fromPhoenix(GenomeHelper genomeHelper) {
//        HBaseToVariantStatsConverter converter = new HBaseToVariantStatsConverter(genomeHelper);
//        return converter::convert;
//    }

    public Map<Integer, Map<Integer, VariantStats>> convert(Result result) {
//        String studyIdStr = String.valueOf(studyConfiguration.getStudyId());

        NavigableMap<byte[], byte[]> map = result.getFamilyMap(columnFamily);
        Map<Integer, Map<Integer, VariantStats>> studyCohortStatsMap = new HashMap<>();

        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            byte[] columnBytes = entry.getKey();
            byte[] value = entry.getValue();
            if (value != null && startsWith(columnBytes, VariantPhoenixHelper.STATS_PREFIX_BYTES)
                    && endsWith(columnBytes, VariantPhoenixHelper.STATS_PROTOBUF_SUFIX_BYTES)) {
                String columnName = Bytes.toString(columnBytes);
                String[] split = columnName.split("_");
                Integer studyId = getStudyId(split);
                Integer cohortId = getCohortId(split);

                Map<Integer, VariantStats> statsMap = studyCohortStatsMap.get(studyId);
                if (statsMap == null) {
                    statsMap = new HashMap<>();
                    studyCohortStatsMap.put(studyId, statsMap);
                }
                statsMap.put(cohortId, convert(value));
            }
        }

        return studyCohortStatsMap;
    }

    public Map<Integer, Map<Integer, VariantStats>> convert(ResultSet resultSet) {
//        String studyIdStr = String.valueOf(studyConfiguration.getStudyId());
        Map<Integer, Map<Integer, VariantStats>> studyCohortStatsMap = new HashMap<>();

        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                byte[] value = resultSet.getBytes(i);
                if (value != null && columnName.startsWith(VariantPhoenixHelper.STATS_PREFIX)
                        && columnName.endsWith(VariantPhoenixHelper.STATS_PROTOBUF_SUFIX)) {
                    String[] split = columnName.split("_");
                    Integer studyId = getStudyId(split);
                    Integer cohortId = getCohortId(split);

                    Map<Integer, VariantStats> statsMap = studyCohortStatsMap.get(studyId);
                    if (statsMap == null) {
                        statsMap = new HashMap<>();
                        studyCohortStatsMap.put(studyId, statsMap);
                    }
                    statsMap.put(cohortId, convert(value));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return studyCohortStatsMap;
    }

    public Integer getStudyId(String[] split) {
        return Integer.valueOf(split[0]);
    }

    public Integer getCohortId(String[] split) {
        return Integer.valueOf(split[1]);
    }

    protected VariantStats convert(byte[] data) {
        VariantStats stats = new VariantStats();
        try {
            VariantProto.VariantStats protoStats = VariantProto.VariantStats.parseFrom(data);

            stats.setMgf(protoStats.getMgf());
            stats.setMgfGenotype(protoStats.getMgfGenotype());
            stats.setMaf(protoStats.getMaf());
            stats.setMafAllele(protoStats.getMafAllele());
            stats.setAltAlleleCount(protoStats.getAltAlleleCount());
            stats.setAltAlleleFreq(protoStats.getAltAlleleFreq());
            stats.setRefAlleleCount(protoStats.getRefAlleleCount());
            stats.setRefAlleleFreq(protoStats.getRefAlleleFreq());

            Map<Genotype, Float> genotypesFreq = new HashMap<>();
            for (Map.Entry<String, Integer> entry : protoStats.getGenotypesCount().entrySet()) {
                Genotype g = new Genotype(entry.getKey());
                stats.addGenotype(g, entry.getValue(), false);
                Float freq = protoStats.getGenotypesFreq().get(entry.getKey());
                if (freq != null) {
                    genotypesFreq.put(g, freq);
                }
            }
            stats.setGenotypesFreq(genotypesFreq);
            stats.setMissingAlleles(protoStats.getMissingAlleles());
            stats.setMissingGenotypes(protoStats.getMissingGenotypes());
            stats.setHw(convert(protoStats.getHw()));

        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        return stats;
    }

    private VariantHardyWeinbergStats convert(VariantProto.VariantHardyWeinbergStats protoHw) {
        if (null == protoHw) {
            return null;
        }
        VariantHardyWeinbergStats hw = new VariantHardyWeinbergStats();
        hw.setChi2(protoHw.getChi2() != -1f ? protoHw.getChi2() : null);
        hw.setPValue(protoHw.getPValue() != -1f ? protoHw.getPValue() : null);

        hw.setN(protoHw.getN() != -1 ? protoHw.getN() : null);

        hw.setNAa00(protoHw.getNAa00() != -1 ? protoHw.getNAa00() : null);
        hw.setNAa10(protoHw.getNAa10() != -1 ? protoHw.getNAa10() : null);
        hw.setNAA11(protoHw.getNAA11() != -1 ? protoHw.getNAA11() : null);

        hw.setEAa00(protoHw.getEAa00() != -1f ? protoHw.getEAa00() : null);
        hw.setEAa10(protoHw.getEAa10() != -1f ? protoHw.getEAa10() : null);
        hw.setEAA11(protoHw.getEAA11() != -1f ? protoHw.getEAA11() : null);

        hw.setP(protoHw.getP() != -1f ? protoHw.getP() : null);
        hw.setQ(protoHw.getQ() != -1f ? protoHw.getQ() : null);
        return hw;
    }

    public boolean startsWith(byte[] bytes, byte[] startsWith) {
        if (bytes.length < startsWith.length) {
            return false;
        }
        for (int i = 0; i < startsWith.length; i++) {
            if (startsWith[i] != bytes[i]) {
                return false;
            }
        }
        return true;
    }

    public boolean endsWith(byte[] bytes, byte[] endsWith) {
        if (bytes.length < endsWith.length) {
            return false;
        }
        for (int i = endsWith.length - 1, f = bytes.length - 1; i >= 0; i--, f--) {
            if (endsWith[i] != bytes[f]) {
                return false;
            }
        }
        return true;
    }

}
