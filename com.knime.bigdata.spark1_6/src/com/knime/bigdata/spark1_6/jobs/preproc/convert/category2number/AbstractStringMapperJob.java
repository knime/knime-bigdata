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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark1_6.jobs.preproc.convert.category2number;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobInput;
import com.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobOutput;
import com.knime.bigdata.spark1_6.base.NamedObjects;
import com.knime.bigdata.spark1_6.base.SparkJob;

/**
 * converts nominal values from a set of columns to numbers and adds corresponding new columns
 *
 * @author dwk
 */
@SparkClass
public abstract class AbstractStringMapperJob implements SparkJob<Category2NumberJobInput, Category2NumberJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(AbstractStringMapperJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public Category2NumberJobOutput runJob(final SparkContext sparkContext, final Category2NumberJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {
        LOGGER.info("starting job to convert nominal values...");
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());
        final String[] colNames = input.getIncludeColNames();
        final Integer[] colIdxs = input.getIncludeColIdxs();
        final int[] colIds = new int[colIdxs.length];
        final Map<Integer, String> colNameForIndex = new HashMap<>();
        int i = 0;
        for (Integer ix : colIdxs) {
            colIds[i] = ix;
            colNameForIndex.put(ix, colNames[i]);
            i++;
        }

        return execute(sparkContext, input, namedObjects, rowRDD, colIds, colNameForIndex);
    }

    /**
     * @param aContext
     * @param aConfig
     * @param namedObjects
     * @param aRowRDD
     * @param aColIds
     * @param aColNameForIndex
     * @return
     * @throws KNIMESparkException
     */
    protected abstract Category2NumberJobOutput execute(final SparkContext aContext, final Category2NumberJobInput aConfig, NamedObjects namedObjects,
        final JavaRDD<Row> aRowRDD, final int[] aColIds, final Map<Integer, String> aColNameForIndex) throws KNIMESparkException;
}
