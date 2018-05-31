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

package org.opencb.opencga.storage.core.variant.io;

import com.google.common.collect.BiMap;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.*;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by jmmut on 2015-06-25.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 * @author Matthias Haimel
 */
public class VariantVcfDataWriter implements DataWriter<Variant> {

    private static final DecimalFormat DECIMAL_FORMAT_7 = new DecimalFormat("#.#######");
    private static final DecimalFormat DECIMAL_FORMAT_3 = new DecimalFormat("#.###");
    protected static final String MINOR_ALLELE_FREQUENCY_KEY = "MAF";
    public static final String CSQ = "CSQ";
    private final Logger logger = LoggerFactory.getLogger(VariantVcfDataWriter.class);


    private static final String DEFAULT_ANNOTATIONS = "allele|gene|ensemblGene|ensemblTranscript|biotype|consequenceType|phastCons|phylop"
            + "|populationFrequency|cDnaPosition|cdsPosition|proteinPosition|sift|polyphen|clinvar|cosmic|gwas|drugInteraction";

    private static final String ALL_ANNOTATIONS = "allele|gene|ensemblGene|ensemblTranscript|biotype|consequenceType|phastCons|phylop"
            + "|populationFrequency|cDnaPosition|cdsPosition|proteinPosition|sift|polyphen|clinvar|cosmic|gwas|drugInteraction";
    private final StudyConfiguration studyConfiguration;
    private final VariantSourceDBAdaptor sourceDBAdaptor;
    private final OutputStream outputStream;
    private final QueryOptions queryOptions;
    private final AtomicReference<Function<String, String>> sampleNameConverter = new AtomicReference<>(s -> s);
    private final int studyId;
    private final String studyName;
    private volatile boolean studyNameAsStudyId = true;
    private VariantContextWriter writer;
    private List<String> annotations;
    private int failedVariants;
    private final List<String> sampleNames = new ArrayList<>();
    private final Map<String, String> sampleNameMapping = new ConcurrentHashMap<>();
    private final AtomicReference<BiConsumer<Variant, RuntimeException>> converterErrorListener = new AtomicReference<>((v, r) -> { });
    private final AtomicBoolean exportGenotype = new AtomicBoolean(true);
    private Supplier<Collection<VCFHeaderLine>> customHeaderSupplier = null;
    private BiFunction<String, VariantStats, Map<String, String>> customAttributeStatsFunction = null;
    private Function<Variant, Map<String, String>> customAttributeFunction = null;
    private Map<String, Integer> cohortIds;
    private Map<String, String> attributeKeyMapping;

    public VariantVcfDataWriter(StudyConfiguration studyConfiguration, VariantSourceDBAdaptor sourceDBAdaptor, OutputStream outputStream,
                                QueryOptions queryOptions) {
        this.studyConfiguration = studyConfiguration;
        this.sourceDBAdaptor = sourceDBAdaptor;
        this.outputStream = outputStream;
        this.queryOptions = queryOptions == null ? new QueryOptions() : queryOptions;
        studyId = this.studyConfiguration.getStudyId();
        studyName = this.studyConfiguration.getStudyName();
        cohortIds = new HashMap<>(studyConfiguration.getCohortIds());
        this.attributeKeyMapping = new HashMap<>();
    }

    public void addAttributeKeyMapping(String from, String to) {
        this.attributeKeyMapping.put(from, to);
    }

    /**
     * The function converts the provided key to a registered value or returns the original input key.
     * @param from input key to map
     * @return String value for key
     */
    public String getAttributeKey(String from) {
        return this.attributeKeyMapping.getOrDefault(from, from);
    }

    public Map<String, Integer> getCohortIds() {
        return cohortIds;
    }

    public void setCohortIds(Map<String, Integer> cohortIds) {
        this.cohortIds = cohortIds;
    }

    public Supplier<Collection<VCFHeaderLine>> getCustomHeaderSupplier() {
        return customHeaderSupplier;
    }

    public void setCustomHeaderSupplier(Supplier<Collection<VCFHeaderLine>> customHeaderSupplier) {
        this.customHeaderSupplier = customHeaderSupplier;
    }

    public Function<Variant, Map<String, String>> getCustomAttributeFunction() {
        return customAttributeFunction;
    }

    public void setCustomAttributeFunction(Function<Variant, Map<String, String>> customAttributeFunction) {
        this.customAttributeFunction = customAttributeFunction;
    }

    public boolean isStudyNameAsStudyId() {
        return studyNameAsStudyId;
    }

    public void setStudyNameAsStudyId(boolean studyNameAsStudyId) {
        this.studyNameAsStudyId = studyNameAsStudyId;
    }

    public void setSampleNameConverter(Function<String, String> converter) {
        sampleNameConverter.set(converter);
    }

    public void setConverterErrorListener(BiConsumer<Variant, RuntimeException> converterErrorListener) {
        this.converterErrorListener.set(converterErrorListener);
    }

    public void setExportGenotype(boolean exportGenotype) {
        this.exportGenotype.set(exportGenotype);
    }

    /**
     * Uses a reader and a writer to dump a vcf.
     * TODO jmmut: use studyConfiguration to know the order of
     *
     * @param adaptor The query adaptor to execute the query
     * @param studyConfiguration Configuration object
     * @param outputUri The destination file
     * @param query The query object
     * @param options The options
     */
    @Deprecated
    public static void vcfExport(VariantDBAdaptor adaptor, StudyConfiguration studyConfiguration, URI outputUri, Query query,
                                 QueryOptions options) {

        // Default objects
        VariantDBReader reader = new VariantDBReader(studyConfiguration, adaptor, query, options);
        org.opencb.biodata.formats.variant.vcf4.io.VariantVcfDataWriter writer =
                new org.opencb.biodata.formats.variant.vcf4.io.VariantVcfDataWriter(reader, outputUri.getPath());
        int batchSize = 100;

        // user tuning
//        if (options != null) {
//            batchSize = options.getInt(VariantStorageEngine.BATCH_SIZE, batchSize);
//        }

        // setup
        reader.open();
        reader.pre();
        writer.open();
        writer.pre();

        // actual loop
        List<Variant> variants = reader.read(batchSize);
//        while (!(variants = reader.read(batchSize)).isEmpty()) {
//            writer.write(variants);
//        }
        while (!variants.isEmpty()) {
            writer.write(variants);
            variants = reader.read(batchSize);
        }


        // tear down
        reader.post();
        reader.close();
        writer.post();
        writer.close();
    }

    @Deprecated
    public static int htsExport(VariantDBIterator iterator, StudyConfiguration studyConfiguration, VariantSourceDBAdaptor sourceDBAdaptor,
                                OutputStream outputStream, QueryOptions queryOptions) {

        VariantVcfDataWriter exporter = new VariantVcfDataWriter(studyConfiguration, sourceDBAdaptor, outputStream, queryOptions);

        exporter.open();
        exporter.pre();

        iterator.forEachRemaining(exporter::write);

        exporter.post();
        exporter.close();
        return exporter.failedVariants;
    }

    @Override
    public boolean pre() {
        LinkedHashSet<VCFHeaderLine> meta = new LinkedHashSet<>();
        sampleNames.clear();
        sampleNames.addAll(getSamples(queryOptions));
        logger.info("Use {} samples for export ... ", this.sampleNames.size());
        sampleNameMapping.putAll(
                sampleNames.stream().collect(Collectors.toMap(s -> s, s -> sampleNameConverter.get().apply(s))));

        List<String> names = sampleNames.stream().map(s -> sampleNameMapping.get(s)).collect(Collectors.toList());
        logger.info("Samples mapped: {} ... ", names.size());

        /* FILTER */
        meta.add(new VCFFilterHeaderLine("PASS", "Valid variant"));
        meta.add(new VCFFilterHeaderLine(".", "No FILTER info"));

        /* INFO */
        meta.add(new VCFInfoHeaderLine(getAttributeKey("PR"), 1, VCFHeaderLineType.Float, "Pass rate"));
        meta.add(new VCFInfoHeaderLine(getAttributeKey("CR"), 1, VCFHeaderLineType.Float, "Call rate"));
        meta.add(new VCFInfoHeaderLine(getAttributeKey("OPR"), 1, VCFHeaderLineType.Float, "Overall Pass rate"));
        addCohortInfo(meta);
        addAnnotationInfo(meta);

        /* FORMAT */
        meta.add(new VCFFormatHeaderLine("GT", 1, VCFHeaderLineType.String, "Genotype"));
        meta.add(new VCFFormatHeaderLine("PF", 1, VCFHeaderLineType.Integer,
                "Variant was PASS (1) filter in original vcf"));

        if (null != this.getCustomHeaderSupplier()) {
            meta.addAll(this.getCustomHeaderSupplier().get()); // add custom headers
        }

        final VCFHeader header = new VCFHeader(meta, names);
        final SAMSequenceDictionary sequenceDictionary = header.getSequenceDictionary();

        // setup writer
        VariantContextWriterBuilder builder = new VariantContextWriterBuilder()
                .setOutputStream(outputStream)
                .setReferenceDictionary(sequenceDictionary)
                .unsetOption(Options.INDEX_ON_THE_FLY);
        if (sampleNames.isEmpty() || !this.exportGenotype.get()) {
            builder.setOption(Options.DO_NOT_WRITE_GENOTYPES);
        }
        List<String> formatFields = studyConfiguration.getAttributes()
                .getAsStringList(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key());
        List<String> formatFieldsType = studyConfiguration.getAttributes()
                .getAsStringList(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key());
        for (int i = 0; i < formatFields.size(); i++) {
            String id = formatFields.get(i);
            if (header.getFormatHeaderLine(id) == null) {
                header.addMetaDataLine(new VCFFormatHeaderLine(id, 1, VCFHeaderLineType.valueOf(formatFieldsType.get(i)), ""));
            }
        }

        writer = builder.build();
        writer.writeHeader(header);
        return true;
    }

    protected void addAnnotationInfo(LinkedHashSet<VCFHeaderLine> meta) {
        // check if variant annotations are exported in the INFO column
        annotations = null;
        if (queryOptions != null && queryOptions.getString("annotations") != null && !queryOptions.getString("annotations").isEmpty()) {
            String annotationString;
            switch (queryOptions.getString("annotations")) {
                case "all":
                    annotationString = ALL_ANNOTATIONS.replaceAll(",", "|");
                    break;
                case "default":
                    annotationString = DEFAULT_ANNOTATIONS.replaceAll(",", "|");
                    break;
                default:
                    annotationString = queryOptions.getString("annotations").replaceAll(",", "|");
                    break;
            }
//            String annotationString = queryOptions.getString("annotations", DEFAULT_ANNOTATIONS).replaceAll(",", "|");
            annotations = Arrays.asList(annotationString.split("\\|"));
            meta.add(new VCFInfoHeaderLine(getAttributeKey(CSQ), 1, VCFHeaderLineType.String, "Consequence annotations from CellBase. "
                    + "Format: " + annotationString));
        }
    }

    protected void addCohortInfo(LinkedHashSet<VCFHeaderLine> meta) {
        for (String cohortName : getCohortIds().keySet()) {
            String prefix = buildCohortPrefix(cohortName);
            String txt = cohortName.equals(StudyEntry.DEFAULT_COHORT) ? StringUtils.EMPTY : cohortName + " cohort: ";

            meta.add(new VCFInfoHeaderLine(prefix + VCFConstants.ALLELE_COUNT_KEY, VCFHeaderLineCount.A,
                    VCFHeaderLineType.Integer, txt + "Total number of alternate alleles in called genotypes,"
                    + " for each ALT allele, in the same order as listed"));
            meta.add(new VCFInfoHeaderLine(prefix + VCFConstants.ALLELE_FREQUENCY_KEY, VCFHeaderLineCount.A,
                    VCFHeaderLineType.Float,
                    txt + "Allele Frequency, for each ALT allele, calculated from AC and AN, in the range (0,1),"
                    + " in the same order as listed"));
            meta.add(new VCFInfoHeaderLine(prefix + VCFConstants.ALLELE_NUMBER_KEY, 1,
                    VCFHeaderLineType.Integer, txt + "Total number of alleles in called genotypes"));
            meta.add(new VCFInfoHeaderLine(prefix + MINOR_ALLELE_FREQUENCY_KEY, VCFHeaderLineCount.A,
                    VCFHeaderLineType.Float,
                    txt + "Minor allele frequency calculated from AC and AN, in the range (0,1) in the same order as listed"));
        }
    }

    public String buildCohortPrefix(String cohortName) {
        return cohortName.equals(StudyEntry.DEFAULT_COHORT) ? StringUtils.EMPTY : cohortName + "_";
    }

    @Override
    public boolean write(List<Variant> batch) {
        for (Variant variant : batch) {
            try {
                VariantContext variantContext = convertVariantToVariantContext(variant, annotations);
                if (variantContext != null) {
                    writer.add(variantContext);
                }
            } catch (RuntimeException e) {
                logger.error("Error exporting variant " + variant, e);
                failedVariants++;
                converterErrorListener.get().accept(variant, e);
            }
        }
        return true;
    }


    @Override
    public boolean post() {
        if (failedVariants > 0) {
            logger.warn(failedVariants + " variants were not written due to errors");
        }
        return true;
    }

    @Override
    public boolean close() {
        writer.close();
        return true;
    }

    private List<String> getReturnedSamples(StudyConfiguration studyConfiguration, QueryOptions options) {
        Map<Integer, List<Integer>> returnedSamplesMap =
                VariantDBAdaptorUtils.getReturnedSamples(new Query(options), options, studyConfiguration);
        List<String> returnedSamples = returnedSamplesMap.get(studyConfiguration.getStudyId()).stream()
                .map(sampleId -> studyConfiguration.getSampleIds().inverse().get(sampleId))
                .collect(Collectors.toList());
        return returnedSamples;
    }

    protected List<String> getSamples(QueryOptions options) {
        if (!this.exportGenotype.get()) {
            logger.info("Do NOT export genotype -> sample list empty!!!");
            return Collections.emptyList();
        }
        // Get Sample names from query & study configuration
        if (options != null) {
            if (options.get(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key()) != null) {
                List<String> queryList = options.getAsStringList(VariantDBAdaptor.VariantQueryParams
                        .RETURNED_SAMPLES.key());
                return queryList.stream().map(s -> {
                    if (StringUtils.isNumeric(s)) {
                        int sampleId = Integer.parseInt(s);
                        if (!studyConfiguration.getSampleIds().inverse().containsKey(sampleId)) {
                            throw new IllegalArgumentException("Unkown sample: " + s + " for study "
                                    + studyConfiguration.getStudyName() + " (" + studyConfiguration.getStudyId() + ")");
                        }
                        return studyConfiguration.getSampleIds().inverse().get(sampleId);
                    } else {
                        if (!studyConfiguration.getSampleIds().containsKey(s)) {
                            throw new IllegalArgumentException("Unkown sample: " + s + " for study "
                                    + studyConfiguration.getStudyName() + " (" + studyConfiguration.getStudyId() + ")");
                        }
                        return s;
                    }
                }).collect(Collectors.toList());
            }
        }
        // Else all from study configuration
        BiMap<Integer, String> samplesPosition = StudyConfiguration.getIndexedSamplesPosition(studyConfiguration).inverse();
        List<String> sampleNames = new ArrayList<>(samplesPosition.size());
        for (int i = 0; i < samplesPosition.size(); i++) {
            sampleNames.add(samplesPosition.get(i));
        }
        return sampleNames;
    }

    /**
     * Convert org.opencb.biodata.models.variant.Variant into a htsjdk.variant.variantcontext.VariantContext
     * some assumptions:
     * * splitted multiallelic variants will produce only one variantContexts. Merging is done
     * * If some normalization has been applied, the source entries may have an attribute ORI like: "POS:REF:ALT_0(,ALT_N)*:ALT_IDX"
     *
     * @param variant A variant object to be converted
     * @param studyConfiguration StudyConfiguration
     * @param annotations Variant annotation
     * @return The variant in HTSJDK format
     * TODO: Move to a separated converter
     */
    public VariantContext convertVariantToVariantContext(Variant variant, StudyConfiguration studyConfiguration,
                                                         List<String> annotations) { //, StudyConfiguration
        return convertVariantToVariantContext(variant, annotations);
    }

    public List<String> buildAlleles(Variant variant, Pair<Integer, Integer> adjustedRange) {
        String reference = variant.getReference();
        String alternate = variant.getAlternate();
        List<AlternateCoordinate> secAlts = getStudy(variant).getSecondaryAlternates();
        List<String> alleles = new ArrayList<>(secAlts.size() + 2);
        Integer origStart = variant.getStart();
        Integer origEnd = variant.getEnd();
        alleles.add(buildAllele(variant.getChromosome(), origStart, origEnd, reference, adjustedRange));
        alleles.add(buildAllele(variant.getChromosome(), origStart, origEnd, alternate, adjustedRange));
        secAlts.forEach(alt -> {
            alleles.add(buildAllele(variant.getChromosome(), alt.getStart(), alt.getEnd(), alt.getAlternate(), adjustedRange));
        });
        return alleles;
    }

    protected StudyEntry getStudy(Variant variant) {
        return variant.getStudy(studyNameAsStudyId ? this.studyName : this.studyConfiguration.getStudyId() + "");
    }

    public String buildAllele(String chromosome, Integer start, Integer end, String allele, Pair<Integer, Integer> adjustedRange) {
        if (start.equals(adjustedRange.getLeft()) && end.equals(adjustedRange.getRight())) {
            return allele; // same start / end
        }
        if (StringUtils.startsWith(allele, "*")) {
            return allele; // no need
        }
        return getReferenceBase(chromosome, adjustedRange.getLeft(), start) + allele
                + getReferenceBase(chromosome, end, adjustedRange.getRight());
    }

    /**
     * Get bases from reference sequence.
     * @param chromosome Chromosome.
     * @param from Start ( inclusive) position.
     * @param to End (exclusive) position.
     * @return String Reference sequence of length to - from.
     */
    private String getReferenceBase(String chromosome, Integer from, Integer to) {
        int length = to - from;
        if (length < 0) {
            throw new IllegalStateException(
                    "Sequence length is negative: chromosome " + chromosome + " from " + from + " to " + to);
        }
        return StringUtils.repeat('N', length); // current return default base TODO load reference sequence
    }

    private static <T> void nonNull(T obj, Consumer<T> consumer) {
        if (Objects.nonNull(obj)) {
            consumer.accept(obj);
        }
    }

    public VariantContext convertVariantToVariantContext(Variant variant, List<String> annotations) { //, StudyConfiguration
        final String noCallAllele = String.valueOf(VCFConstants.NO_CALL_ALLELE);
        VariantContextBuilder variantContextBuilder = new VariantContextBuilder();
        VariantType type = variant.getType();
        Pair<Integer, Integer> adjustedRange = adjustedVariantStart(variant);
        List<String> allelesArray = buildAlleles(variant, adjustedRange);
        Set<Integer> nocallAlleles = IntStream.range(0,  allelesArray.size()).boxed()
                .filter(i -> noCallAllele.equals(allelesArray.get(i)))
                .collect(Collectors.toSet());
        String filter = "PASS";

        //Attributes for INFO column
        ObjectMap attributes = new ObjectMap();
        ArrayList<Genotype> genotypes = new ArrayList<>();
        StudyEntry studyEntry = getStudy(variant);

//        Integer originalPosition = null;
//        List<String> originalAlleles = null;
        // TODO work out properly how to deal with multi allelic sites.
//        String[] ori = getOri(studyEntry);
//        Integer auxOriginalPosition = getOriginalPosition(ori);
//        if (originalPosition != null && auxOriginalPosition != null && !originalPosition.equals(auxOriginalPosition)) {
//            throw new IllegalStateException("Two or more VariantSourceEntries have different origin. Unable to merge");
//        }
//        originalPosition = auxOriginalPosition;
//        originalAlleles = getOriginalAlleles(ori);
//        if (originalAlleles == null) {
//            originalAlleles = allelesArray;
//        }
//
//        //Only print those variants in which the alternate is the first alternate from the multiallelic alternatives
//        if (originalAlleles.size() > 2 && !"0".equals(getOriginalAlleleIndex(ori))) {
//            logger.debug("Skip multi allelic variant! " + variant);
//            return null;
//        }

        String sourceFilter = studyEntry.getAttribute("FILTER");
        if (sourceFilter != null && !filter.equals(sourceFilter)) {
            filter = ".";   // write PASS iff all sources agree that the filter is "PASS" or assumed if not present, otherwise write "."
        }

        String refAllele = allelesArray.get(0);
        for (String sampleName : this.sampleNames) {
            String gtStr = studyEntry.getSampleData(sampleName, "GT");
            String genotypeFilter = studyEntry.getSampleData(sampleName, "FT");

            if (Objects.isNull(gtStr)) {
                gtStr = noCallAllele;
                genotypeFilter = noCallAllele;
            }

            List<String> gtSplit = new ArrayList<>(Arrays.asList(gtStr.split(",")));
            List<String> ftSplit = new ArrayList<>(Arrays.asList(
                    (StringUtils.isBlank(genotypeFilter) ? "" : genotypeFilter).split(",")));
            while (gtSplit.size() > 1) {
                int idx = gtSplit.indexOf(noCallAllele);
                if (idx < 0) {
                    idx = gtSplit.indexOf("0/0");
                }
                if (idx < 0) {
                    break;
                }
                gtSplit.remove(idx);
                ftSplit.remove(idx);
            }
            String gt = gtSplit.get(0);
            String ft = ftSplit.get(0);

            org.opencb.biodata.models.feature.Genotype genotype =
                    new org.opencb.biodata.models.feature.Genotype(gt, refAllele, allelesArray.subList(1, allelesArray.size()));
            List<Allele> alleles = new ArrayList<>();
            for (int gtIdx : genotype.getAllelesIdx()) {
                if (gtIdx < allelesArray.size() && gtIdx >= 0 && !nocallAlleles.contains(gtIdx)) { // .. AND NOT a nocall allele
                    alleles.add(Allele.create(allelesArray.get(gtIdx), gtIdx == 0)); // allele is ref. if the alleleIndex is 0
                } else {
                    alleles.add(Allele.create(noCallAllele, false)); // genotype of a secondary alternate, or an actual missing
                }
            }

            if (StringUtils.isBlank(ft)) {
                genotypeFilter = null;
            } else if (StringUtils.equals("PASS", ft)) {
                genotypeFilter = "1";
            } else {
                genotypeFilter = "0";
            }
            GenotypeBuilder builder = new GenotypeBuilder()
                    .name(this.sampleNameMapping.get(sampleName));
            if (studyEntry.getFormatPositions().containsKey("GT")) {
                builder.alleles(alleles)
                        .phased(genotype.isPhased());
            }
            if (genotypeFilter != null) {
                builder.attribute("PF", genotypeFilter);
            }
            for (String id : studyEntry.getFormat()) {
                if (id.equals("GT") || id.equals("FT")) {
                    continue;
                }
                String value = studyEntry.getSampleData(sampleName, id);
                builder.attribute(id, value);
            }

            genotypes.add(builder.make());
        }

        addStats(studyEntry, attributes);

        variantContextBuilder.start(adjustedRange.getLeft())
                .stop(adjustedRange.getLeft() + refAllele.length() - 1) //TODO mh719: check what happens for Insertions
                .chr(variant.getChromosome())
                .filter(filter); // TODO jmmut: join attributes from different source entries? what to do on a collision?

        if (genotypes.isEmpty()) {
            variantContextBuilder.noGenotypes();
        } else {
            variantContextBuilder.genotypes(genotypes);
        }

        if (type.equals(VariantType.NO_VARIATION) && allelesArray.get(1).isEmpty()) {
            variantContextBuilder.alleles(refAllele);
        } else {
            variantContextBuilder.alleles(allelesArray.stream().filter(a -> !a.equals(noCallAllele)).collect(Collectors.toList()));
        }

        // if asked variant annotations are exported
        if (annotations != null) {
            addAnnotations(variant, annotations, attributes);
        }

        if (null != this.getCustomAttributeFunction()) {
            attributes.putAll(getCustomAttributeFunction().apply(variant));
        }

        variantContextBuilder.attributes(attributes);

        if (StringUtils.isNotEmpty(variant.getId()) && !variant.toString().equals(variant.getId())) {
            StringBuilder ids = new StringBuilder();
            ids.append(variant.getId());
            if (variant.getNames() != null) {
                for (String name : variant.getNames()) {
                    ids.append(VCFConstants.ID_FIELD_SEPARATOR).append(name);
                }
            }
            variantContextBuilder.id(ids.toString());
        } else {
            variantContextBuilder.id(VCFConstants.EMPTY_ID_FIELD);
        }

        return variantContextBuilder.make();
    }

    /**
     * Adjust start/end if a reference base is required due to an empty allele. All variants are checked due to SecAlts.
     * @param variant {@link Variant} object.
     * @return Pair<Integer, Integer> The adjusted (or same) start/end position e.g. SV and MNV as SecAlt, INDEL, etc.
     */
    protected Pair<Integer, Integer> adjustedVariantStart(Variant variant) {
        Integer start = variant.getStart();
        Integer end = variant.getEnd();
        if (StringUtils.isBlank(variant.getReference()) || StringUtils.isBlank(variant.getAlternate())) {
            start = start - 1;
        }
        for (AlternateCoordinate alternateCoordinate : getStudy(variant).getSecondaryAlternates()) {
            start = Math.min(start, alternateCoordinate.getStart());
            end = Math.max(end, alternateCoordinate.getEnd());
            if (StringUtils.isBlank(alternateCoordinate.getAlternate()) || StringUtils.isBlank(alternateCoordinate.getReference())) {
                start = Math.min(start, alternateCoordinate.getStart() - 1);
            }
        }
        return new ImmutablePair<>(start, end);
    }

    private Map<String, Object> addAnnotations(Variant variant, List<String> annotations, Map<String, Object> attributes) {
        StringBuilder stringBuilder = new StringBuilder();
        if (variant.getAnnotation() == null) {
            return attributes;
        }
//        for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
        for (int i = 0; i < variant.getAnnotation().getConsequenceTypes().size(); i++) {
            ConsequenceType consequenceType = variant.getAnnotation().getConsequenceTypes().get(i);
            for (int j = 0; j < annotations.size(); j++) {
                switch (annotations.get(j)) {
                    case "allele":
                        stringBuilder.append(variant.getAlternate());
                        break;
                    case "consequenceType":
                        stringBuilder.append(consequenceType.getSequenceOntologyTerms().stream()
                                .map(SequenceOntologyTerm::getName).collect(Collectors.joining("&")));
                        break;
                    case "gene":
                        if (consequenceType.getGeneName() != null) {
                            stringBuilder.append(consequenceType.getGeneName());
                        }
                        break;
                    case "ensemblGene":
                        if (consequenceType.getEnsemblGeneId() != null) {
                            stringBuilder.append(consequenceType.getEnsemblGeneId());
                        }
                        break;
                    case "ensemblTranscript":
                        if (consequenceType.getEnsemblTranscriptId() != null) {
                            stringBuilder.append(consequenceType.getEnsemblTranscriptId());
                        }
                        break;
                    case "biotype":
                        if (consequenceType.getBiotype() != null) {
                            stringBuilder.append(consequenceType.getBiotype());
                        }
                        break;
                    case "phastCons":
                        if (variant.getAnnotation().getConservation() != null) {
                            List<Double> phastCons = variant.getAnnotation().getConservation().stream()
                                    .filter(t -> t.getSource().equalsIgnoreCase("phastCons"))
                                    .map(Score::getScore)
                                    .collect(Collectors.toList());
                            if (phastCons.size() > 0) {
                                stringBuilder.append(DECIMAL_FORMAT_3.format(phastCons.get(0)));
                            }
                        }
                        break;
                    case "phylop":
                        if (variant.getAnnotation().getConservation() != null) {
                            List<Double> phylop = variant.getAnnotation().getConservation().stream()
                                    .filter(t -> t.getSource().equalsIgnoreCase("phylop"))
                                    .map(Score::getScore)
                                    .collect(Collectors.toList());
                            if (phylop.size() > 0) {
                                stringBuilder.append(DECIMAL_FORMAT_3.format(phylop.get(0)));
                            }
                        }
                        break;
                    case "populationFrequency":
                        List<PopulationFrequency> populationFrequencies = variant.getAnnotation().getPopulationFrequencies();
                        if (populationFrequencies != null) {
                            stringBuilder.append(populationFrequencies.stream()
                                    .map(t -> t.getPopulation() + ":" + t.getAltAlleleFreq())
                                    .collect(Collectors.joining("&")));
                        }
                        break;
                    case "cDnaPosition":
                        stringBuilder.append(consequenceType.getCdnaPosition());
                        break;
                    case "cdsPosition":
                        stringBuilder.append(consequenceType.getCdsPosition());
                        break;
                    case "proteinPosition":
                        if (consequenceType.getProteinVariantAnnotation() != null) {
                            stringBuilder.append(consequenceType.getProteinVariantAnnotation().getPosition());
                        }
                        break;
                    case "sift":
                        if (consequenceType.getProteinVariantAnnotation() != null
                                && consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {
                            List<Double> sift = consequenceType.getProteinVariantAnnotation().getSubstitutionScores().stream()
                                    .filter(t -> t.getSource().equalsIgnoreCase("sift"))
                                    .map(Score::getScore)
                                    .collect(Collectors.toList());
                            if (sift.size() > 0) {
                                stringBuilder.append(DECIMAL_FORMAT_3.format(sift.get(0)));
                            }
                        }
                        break;
                    case "polyphen":
                        if (consequenceType.getProteinVariantAnnotation() != null
                                && consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {
                            List<Double> polyphen = consequenceType.getProteinVariantAnnotation().getSubstitutionScores().stream()
                                    .filter(t -> t.getSource().equalsIgnoreCase("polyphen"))
                                    .map(Score::getScore)
                                    .collect(Collectors.toList());
                            if (polyphen.size() > 0) {
                                stringBuilder.append(DECIMAL_FORMAT_3.format(polyphen.get(0)));
                            }
                        }
                        break;
                    case "clinvar":
                        if (variant.getAnnotation().getVariantTraitAssociation() != null
                                && variant.getAnnotation().getVariantTraitAssociation().getClinvar() != null) {
                            stringBuilder.append(variant.getAnnotation().getVariantTraitAssociation().getClinvar().stream()
                                    .map(ClinVar::getTraits).flatMap(Collection::stream)
                                    .collect(Collectors.joining("&")));
                        }
                        break;
                    case "cosmic":
                        if (variant.getAnnotation().getVariantTraitAssociation() != null
                                && variant.getAnnotation().getVariantTraitAssociation().getCosmic() != null) {
                            stringBuilder.append(variant.getAnnotation().getVariantTraitAssociation().getCosmic().stream()
                                    .map(Cosmic::getPrimarySite)
                                    .collect(Collectors.joining("&")));
                        }
                        break;
                    case "gwas":
                        if (variant.getAnnotation().getVariantTraitAssociation() != null
                                && variant.getAnnotation().getVariantTraitAssociation().getGwas() != null) {
                            stringBuilder.append(variant.getAnnotation().getVariantTraitAssociation().getGwas().stream()
                                    .map(Gwas::getTraits).flatMap(Collection::stream)
                                    .collect(Collectors.joining("&")));
                        }
                        break;
                    case "drugInteraction":
                        stringBuilder.append(variant.getAnnotation().getGeneDrugInteraction().stream()
                                .map(GeneDrugInteraction::getDrugName).collect(Collectors.joining("&")));
                        break;
                    default:
                        logger.error("Unknown annotation: " + annotations.get(j));
                        break;
                }
                if (j < annotations.size() - 1) {
                    stringBuilder.append("|");
                }
            }
            if (i < variant.getAnnotation().getConsequenceTypes().size() - 1) {
                stringBuilder.append(",");
            }
        }

        attributes.put(getAttributeKey(CSQ), stringBuilder.toString());
        return attributes;
    }

    private void addStats(StudyEntry studyEntry, Map<String, Object> attributes) {
        if (studyEntry.getStats() == null) {
            return;
        }
        this.cohortIds.forEach((cohortName, cohortId) -> {
            VariantStats stats = studyEntry.getStats().get(cohortName);
            if (null == stats) {
                return;
            }
            String prefix = buildCohortPrefix(cohortName);
            int an = stats.getAltAlleleCount() + stats.getRefAlleleCount();
            attributes.put(prefix + VCFConstants.ALLELE_NUMBER_KEY, String.valueOf(an));
            attributes.put(prefix + VCFConstants.ALLELE_COUNT_KEY, String.valueOf(stats.getAltAlleleCount()));

            attributes.put(prefix + VCFConstants.ALLELE_FREQUENCY_KEY, DECIMAL_FORMAT_7.format(stats.getAltAlleleFreq()));
            attributes.put(prefix + MINOR_ALLELE_FREQUENCY_KEY, DECIMAL_FORMAT_7.format(stats.getMaf()));
            if (null != getCustomAttributeStatsFunction()) {
                attributes.putAll(getCustomAttributeStatsFunction().apply(cohortName, stats));
            }
        });
    }

    public BiFunction<String, VariantStats, Map<String, String>> getCustomAttributeStatsFunction() {
        return customAttributeStatsFunction;
    }

    public void setCustomAttributeStatsFunction(BiFunction<String, VariantStats, Map<String, String>> customAttributeStatsFunction) {
        this.customAttributeStatsFunction = customAttributeStatsFunction;
    }

    /**
     * Assumes that ori is in the form "POS:REF:ALT_0(,ALT_N)*:ALT_IDX".
     * ALT_N is the n-th allele if this is the n-th variant resultant of a multiallelic vcf row
     *
     * @param ori
     * @return
     */
    private static List<String> getOriginalAlleles(String[] ori) {
        if (ori != null && ori.length == 4) {
            String[] multiAllele = ori[2].split(",");
            if (multiAllele.length != 1) {
                ArrayList<String> alleles = new ArrayList<>(multiAllele.length + 1);
                alleles.add(ori[1]);
                alleles.addAll(Arrays.asList(multiAllele));
                return alleles;
            } else {
                return Arrays.asList(ori[1], ori[2]);
            }
        }

        return null;
    }

    private static String getOriginalAlleleIndex(String[] ori) {
        if (ori != null && ori.length == 4) {
            return ori[3];
        }
        return null;
    }

    /**
     * Assumes that ori is in the form "POS:REF:ALT_0(,ALT_N)*:ALT_IDX".
     *
     * @param ori
     * @return
     */
    private static Integer getOriginalPosition(String[] ori) {

        if (ori != null && ori.length == 4) {
            return Integer.parseInt(ori[0]);
        }

        return null;
    }

    private static String[] getOri(StudyEntry studyEntry) {

        List<FileEntry> files = studyEntry.getFiles();
        if (!files.isEmpty()) {
            String call = files.get(0).getCall();
            if (call != null && !call.isEmpty()) {
                return call.split(":");
            }
        }
        return null;
    }


    /**
     * Unclosable output stream.
     *
     * Avoid passing System.out directly to HTSJDK, because it will close it at the end.
     *
     * http://stackoverflow.com/questions/8941298/system-out-closed-can-i-reopen-it/23791138#23791138
     */
    public static class UnclosableOutputStream extends FilterOutputStream {

        public UnclosableOutputStream(OutputStream os) {
            super(os);
        }

        @Override
        public void close() throws IOException {
            super.flush();
        }
    }
}

