package org.opencb.opencga.storage.hadoop.variant.exporters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantVcfDataWriter;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.BiConsumer;

/**
 * Created by mh719 on 21/12/2016.
 * @author Matthias Haimel
 */
public class HadoopVcfOutputFormat extends FileOutputFormat<Variant, NullWritable> {

    public HadoopVcfOutputFormat() {
        // do nothing
    }

    @Override
    public RecordWriter<Variant, NullWritable> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {

        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class file = getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(file, conf);
            extension = codec.getDefaultExtension();
        }
        Path file1 = this.getDefaultWorkFile(job, extension);
        FileSystem fs = file1.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file1, false);
        if (!isCompressed) {
            return new HadoopVcfOutputFormat.VcfRecordWriter(configureWriter(job, fileOut));
        } else {
            DataOutputStream out = new DataOutputStream(codec.createOutputStream(fileOut));
            return new HadoopVcfOutputFormat.VcfRecordWriter(configureWriter(job, out));
        }
    }

    protected VariantVcfDataWriter configureWriter(final TaskAttemptContext job, OutputStream fileOut) {
        job.getCounter(VariantVcfDataWriter.class.getName(), "failed").increment(0); // init
        final Configuration conf = job.getConfiguration();
        try (VariantTableHelper helper = new VariantTableHelper(conf)) {
            BiConsumer<Variant, RuntimeException> failed = (v, e) ->
                    job.getCounter(VariantVcfDataWriter.class.getName(), "failed").increment(1);
            StudyConfiguration sc = helper.loadMeta();
            VariantVcfDataWriter exporter = prepareVcfWriter(helper, sc, failed, fileOut);
            exporter.open();
            exporter.pre();
            return exporter;
        } catch (IOException e) {
            throw new IllegalStateException("Problem init Helper", e);
        }

    }

    protected VariantVcfDataWriter prepareVcfWriter(VariantTableHelper helper, StudyConfiguration sc,
                                                    BiConsumer<Variant, RuntimeException> failed,
                                                    OutputStream fileOut) throws IOException {
        boolean withGenotype = helper.getConf()
                .getBoolean(VariantTableExportDriver.CONFIG_VARIANT_TABLE_EXPORT_GENOTYPE, false);
        VariantSourceDBAdaptor source = new HadoopVariantSourceDBAdaptor(helper);
        QueryOptions options = new QueryOptions();
        VariantVcfDataWriter exporter = new VariantVcfDataWriter(sc, source, fileOut, options);
        exporter.setExportGenotype(withGenotype);
        if (null != failed) {
            exporter.setConverterErrorListener(failed);
        }
        return exporter;
    }

    protected static class VcfRecordWriter extends RecordWriter<Variant, NullWritable> {
        private final VariantVcfDataWriter writer;

        public VcfRecordWriter(VariantVcfDataWriter writer) {
            this.writer = writer;
        }

        @Override
        public void write(Variant variant, NullWritable nullWritable) throws IOException, InterruptedException {
            writer.write(variant);
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            writer.post();
            writer.close();
        }
    }



}
