package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.optimization.L1Updater;
import org.apache.spark.mllib.regression.LabeledPoint;

import com.knime.bigdata.spark.jobserver.server.JobConfig;

/**
 * @author Tobias Koetter, KNIME.com
 */
public class SVMLearnerJob extends SGDJob {

    private final static Logger LOGGER = Logger.getLogger(SVMLearnerJob.class.getName());

    /**
     * @param sc
     * @param aConfig
     * @param inputRdd
     * @return
     */
    @Override
    Serializable execute(final SparkContext sc, final JobConfig aConfig, final JavaRDD<LabeledPoint> inputRdd) {
        final int noOfIteration = getNumIterations(aConfig);
        final SVMWithSGD svmAlg = new SVMWithSGD();
        svmAlg.optimizer().setNumIterations(noOfIteration).setRegParam(getRegularization(aConfig))
            .setUpdater(new L1Updater());
        return svmAlg.run(inputRdd.rdd().cache());
    }


    /**
     * {@inheritDoc}
     */
    @Override
    Logger getLogger() {
        return LOGGER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String getAlgName() {
        return "SVM learner";
    }
}