/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package com.knime.bigdata.spark2_0.jobs.scorer;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.scorer.accuracy.ScorerJobInput;
import com.knime.bigdata.spark2_0.api.NamedObjects;
import com.knime.bigdata.spark2_0.api.SparkJob;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public abstract class AbstractScorerJob implements SparkJob<ScorerJobInput, JobOutput>{
    private static final long serialVersionUID = 1L;

    /** @return local logger instance */
    protected abstract Logger getLogger();

    /** @return Name of this scorer */
    protected abstract String getScorerName();

    /**
     * {@inheritDoc}
     */
    @Override
    public JobOutput runJob(final SparkContext sparkContext, final ScorerJobInput input, final NamedObjects namedObjects) throws KNIMESparkException {
        getLogger().info("Starting " + getScorerName() + " scorer job...");

        final JavaRDD<Row> rowRDD = namedObjects.getDataFrame(input.getFirstNamedInputObject()).javaRDD();
        JobOutput res = doScoring(input,rowRDD);

        getLogger().info(getScorerName() + " scorer job done.");
        return res;
    }

    /**
     * @param input
     * @param rowRDD
     * @return Scorer result
     */
    protected abstract JobOutput doScoring(final ScorerJobInput input, final JavaRDD<Row> rowRDD);
}