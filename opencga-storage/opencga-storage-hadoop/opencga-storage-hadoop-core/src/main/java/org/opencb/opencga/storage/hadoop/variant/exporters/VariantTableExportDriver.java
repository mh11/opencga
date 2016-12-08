package org.opencb.opencga.storage.hadoop.variant.exporters;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;

import java.io.IOException;

/**
 * Created by mh719 on 21/11/2016.
 */
public class VariantTableExportDriver extends AbstractAnalysisTableDriver {
    public static final String CONFIG_VARIANT_TABLE_EXPORT_AVRO_PATH = "opencga.variant.table.export.avro.path";
    public static final String CONFIG_VARIANT_TABLE_EXPORT_AVRO_GENOTYPE = "opencga.variant.table.export.avro.genotype";
    private String outFile;

    public VariantTableExportDriver() { /* nothing */ }

    public VariantTableExportDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        outFile = getConf().get(CONFIG_VARIANT_TABLE_EXPORT_AVRO_PATH, StringUtils.EMPTY);
        if (StringUtils.isEmpty(outFile)) {
            throw new IllegalArgumentException("No AVRO output file specified!!!");
        }
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return AnalysisToAvroMapper.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        super.initMapReduceJob(inTable, job, scan, addDependencyJar);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroKeyOutputFormat.setOutputPath(job, new Path(this.outFile)); // set Path
        AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema()); // Set schema
        AvroKeyOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // compression
        job.setNumReduceTasks(0);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new VariantTableExportDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}