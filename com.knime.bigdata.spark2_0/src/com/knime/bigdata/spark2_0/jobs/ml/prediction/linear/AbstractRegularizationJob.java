package com.knime.bigdata.spark2_0.jobs.ml.prediction.linear;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.ModelJobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.ml.prediction.linear.LinearLearnerJobInput;
import com.knime.bigdata.spark2_0.api.NamedObjects;
import com.knime.bigdata.spark2_0.api.SparkJob;
import com.knime.bigdata.spark2_0.api.SupervisedLearnerUtils;

/**
 * @author dwk
 * @param <I> {@link LinearLearnerJobInput} implementation
 */
@SparkClass
public abstract class AbstractRegularizationJob<I extends LinearLearnerJobInput>
    implements SparkJob<I, ModelJobOutput> {

    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelJobOutput runJob(final SparkContext aSparkContext, final I input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {

        getLogger().info("starting " + getAlgName() + " job...");

        //note that the column in the input RDD should be normalized into 0-1 ranges
        Dataset<Row> trainingData = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        trainingData.cache();
        ModelJobOutput model = SupervisedLearnerUtils.constructAndExecutePipeline(aSparkContext, input, trainingData,
            getConfiguredRegressor(input), false);

        evaluateModel(trainingData, model.getModel());

        trainingData.unpersist();

        getLogger().info(getAlgName() + " done");

        return model;
    }

    protected abstract LinearRegression getConfiguredRegressor(final I input);

    protected abstract double evaluateModel(final Dataset<Row> aData, final Serializable aModel);

    /**
     * @return the configured logger
     */
    protected abstract Logger getLogger();

    /**
     * @return name of this learner (primarily for logging)
     */
    protected abstract String getAlgName();
}