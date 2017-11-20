package org.knime.bigdata.spark1_3.jobs.preproc.normalize;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.preproc.normalize.NormalizeJobInput;
import org.knime.bigdata.spark.node.preproc.normalize.NormalizeJobOutput;
import org.knime.bigdata.spark1_3.api.NamedObjects;
import org.knime.bigdata.spark1_3.api.NormalizedRDDContainer;
import org.knime.bigdata.spark1_3.api.RDDUtilsInJava;
import org.knime.bigdata.spark1_3.api.SparkJob;

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
        final Integer[] cols = input.getIncludeColIdxs();
        final NormalizedRDDContainer normalizeContainer = RDDUtilsInJava.normalize(rowRDD, Arrays.asList(cols), input.getNormalizationSettings());
        JavaRDD<Row> normalizedRDD = normalizeContainer.normalizeRDD(rowRDD, Arrays.asList(cols));
        namedObjects.addJavaRdd(input.getFirstNamedOutputObject(), normalizedRDD);
        final NormalizeJobOutput output = new NormalizeJobOutput(normalizeContainer.getScales(), normalizeContainer.getTranslations());
        LOGGER.info("done");
        return output;
    }
}