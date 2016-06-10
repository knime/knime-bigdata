package com.knime.bigdata.spark1_5.jobs.preproc.normalize;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.preproc.normalize.NormalizeJobInput;
import com.knime.bigdata.spark.node.preproc.normalize.NormalizeJobOutput;
import com.knime.bigdata.spark1_5.base.NamedObjects;
import com.knime.bigdata.spark1_5.base.RDDUtilsInJava;
import com.knime.bigdata.spark1_5.base.SparkJob;

/**
 * @author dwk
 */
@SparkClass
public class NormalizeColumnsJob implements SparkJob<NormalizeJobInput, NormalizeJobOutput> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(NormalizeColumnsJob.class.getName());

    /**
     * run the actual job, the result is serialized back to the client
     *
     * @throws KNIMESparkException
     */
    @Override
    public NormalizeJobOutput runJob(final SparkContext sparkContext, final NormalizeJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {
        LOGGER.info("starting normalization job...");

        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());
        System.out.println("Prior execute");
        final Integer[] cols = input.getIncludeColIdxs();
        final NormalizedRDDContainer normalizeContainer = RDDUtilsInJava.normalize(rowRDD, Arrays.asList(cols), input.getNormalizationSettings());
        JavaRDD<Row> normalizedRDD = normalizeContainer.normalizeRDD(rowRDD, Arrays.asList(cols));
        System.out.println("After execute");
        namedObjects.addJavaRdd(input.getFirstNamedOutputObject(), normalizedRDD);
        System.out.println("Prior output");
        final NormalizeJobOutput output = new NormalizeJobOutput(normalizeContainer.getScales(), normalizeContainer.getTranslations());
        System.out.println("After output");
        LOGGER.info("done");
        return output;
    }
}