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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark3_0.jobs.preproc.convert.category2number;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobInput;
import org.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobOutput;
import org.knime.bigdata.spark3_0.api.NamedObjects;
import org.knime.bigdata.spark3_0.api.SparkJob;

/**
 * converts nominal values from a set of columns to numbers and adds corresponding new columns
 *
 * @author dwk
 */
@SparkClass
public abstract class AbstractStringMapperJob implements SparkJob<Category2NumberJobInput, Category2NumberJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(AbstractStringMapperJob.class.getName());

    @Override
    public Category2NumberJobOutput runJob(final SparkContext sparkContext, final Category2NumberJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        LOGGER.info("Starting job to convert nominal values...");
        final Dataset<Row> dataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final int[] colIdxs = input.getIncludeColIdxs();
        return execute(sparkContext, input, namedObjects, dataset, colIdxs);
    }

    /**
     * @param context - current context
     * @param config - job configuration
     * @param namedObjects - named objects
     * @param dataset - input dataset
     * @param colIds - included column indices
     * @return job output
     * @throws KNIMESparkException
     */
    protected abstract Category2NumberJobOutput execute(final SparkContext context, final Category2NumberJobInput config,
        final NamedObjects namedObjects, final Dataset<Row> dataset, final int[] colIds) throws KNIMESparkException;
}
