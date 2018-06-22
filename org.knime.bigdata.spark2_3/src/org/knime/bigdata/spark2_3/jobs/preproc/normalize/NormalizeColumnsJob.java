package org.knime.bigdata.spark2_3.jobs.preproc.normalize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.preproc.normalize.NormalizeJobInput;
import org.knime.bigdata.spark.node.preproc.normalize.NormalizeJobOutput;
import org.knime.bigdata.spark2_3.api.NamedObjects;
import org.knime.bigdata.spark2_3.api.NormalizedDataFrameContainer;
import org.knime.bigdata.spark2_3.api.NormalizedDataFrameContainerFactory;
import org.knime.bigdata.spark2_3.api.RDDUtilsInJava;
import org.knime.bigdata.spark2_3.api.SparkJob;

/**
 * @author dwk
 */
@SparkClass
public class NormalizeColumnsJob implements SparkJob<NormalizeJobInput, NormalizeJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(NormalizeColumnsJob.class.getName());

    @Override
    public NormalizeJobOutput runJob(final SparkContext sparkContext, final NormalizeJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException {

        LOGGER.info("Starting normalization job...");

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final Dataset<Row> inputDataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final Integer[] cols = input.getIncludeColIdxs();
        final MultivariateStatisticalSummary stats = findColumnStats(inputDataset, Arrays.asList(cols));
        final NormalizedDataFrameContainer normalizeContainer =
              NormalizedDataFrameContainerFactory.getNormalizedRDDContainer(stats, input.getNormalizationSettings());

        final Dataset<Row> normalizedDataset = normalizeContainer.normalize(spark, inputDataset, Arrays.asList(cols));
        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), normalizedDataset);

        LOGGER.info("Normalizer job done.");
        return new NormalizeJobOutput(normalizeContainer.getScales(), normalizeContainer.getTranslations());
    }


    /**
     * Convert given dataset to an RDD<Vector> with selected columns and compute statistics for these columns
     *
     * @param inputDataset
     * @param aColumnIndices
     * @return MultivariateStatisticalSummary
     */
    private MultivariateStatisticalSummary findColumnStats(final Dataset<Row> inputDataset,
        final Collection<Integer> aColumnIndices) {

        List<Integer> columnIndices = new ArrayList<>();
        columnIndices.addAll(aColumnIndices);
        Collections.sort(columnIndices);

        JavaRDD<Vector> mat = RDDUtilsInJava.toVectorRdd(inputDataset, columnIndices);

        // Compute column summary statistics.
        MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
        return summary;
    }
}