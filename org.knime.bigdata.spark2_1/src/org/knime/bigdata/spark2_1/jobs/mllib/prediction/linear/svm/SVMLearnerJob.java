package org.knime.bigdata.spark2_1.jobs.mllib.prediction.linear.svm;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.prediction.linear.LinearLearnerJobInput;
import org.knime.bigdata.spark2_1.jobs.mllib.prediction.linear.AbstractRegularizationJob;

/**
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class SVMLearnerJob extends AbstractRegularizationJob<LinearLearnerJobInput> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(SVMLearnerJob.class.getName());

    /**
     * @param sc
     * @param aConfig
     * @param inputRdd
     * @return
     */
    @Override
    protected Serializable execute(final SparkContext sc, final LinearLearnerJobInput aConfig, final JavaRDD<LabeledPoint> inputRdd) {
        final SVMWithSGD svmAlg = new SVMWithSGD();
        svmAlg.setFeatureScaling(aConfig.useFeatureScaling()).setIntercept(aConfig.addIntercept())
        .setValidateData(aConfig.validateData());
        configureSGDOptimizer(aConfig, svmAlg.optimizer());
        return svmAlg.run(inputRdd.rdd().cache());
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getAlgName() {
        return "SVM learner";
    }
}