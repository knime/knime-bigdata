/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on May 17, 2016 by oole
 */
package com.knime.bigdata.spark1_3.jobs.scorer;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.scorer.accuracy.ScorerJobInput;
import com.knime.bigdata.spark1_3.api.NamedObjects;
import com.knime.bigdata.spark1_3.api.SparkJob;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public abstract class AbstractScorerJob implements SparkJob<ScorerJobInput, JobOutput>{

    private static final long serialVersionUID = 1L;

    protected static final Logger LOGGER = Logger.getLogger(AbstractScorerJob.class.getName());

    /**
     *
     */
    public AbstractScorerJob() {
        super();
    }

    Logger getLogger() {
        return LOGGER;
    }

    String getAlgName() {
        return "Scorer";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobOutput runJob(final SparkContext sparkContext, final ScorerJobInput input, final NamedObjects namedObjects) throws KNIMESparkException {
        getLogger().log(Level.INFO, "START " + getAlgName() + " job...");

        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());
        JobOutput res = doScoring(input,rowRDD);

        getLogger().log(Level.INFO, "DONE " + getAlgName() + " job...");
        return res;
    }


    /**
     * @param input
     * @param rowRDD
     * @return
     */
    protected abstract JobOutput doScoring(final ScorerJobInput input, final JavaRDD<Row> rowRDD);

}