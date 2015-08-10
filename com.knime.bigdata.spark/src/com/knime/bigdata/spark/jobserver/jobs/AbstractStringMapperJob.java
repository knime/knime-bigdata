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
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.Row;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;

/**
 * converts nominal values from a set of columns to numbers and adds corresponding new columns
 *
 * @author dwk
 */
public abstract class AbstractStringMapperJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    static final String PARAM_INPUT_TABLE = ParameterConstants.PARAM_TABLE_1;

    static final String PARAM_COL_NAMES = ParameterConstants.PARAM_COL_IDXS + ParameterConstants.PARAM_STRING;

    private final static Logger LOGGER = Logger.getLogger(AbstractStringMapperJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    String validateParam(final JobConfig aConfig) {

        if (!aConfig.hasInputParameter(PARAM_INPUT_TABLE)) {
            return "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }

        final String msg = SupervisedLearnerUtils.checkSelectedColumnIdsParameter(aConfig);

        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_COL_NAMES)) {
                return "Input parameter '" + PARAM_COL_NAMES + "' missing.";
            } else {
                try {
                    List<String> vals = aConfig.getInputListParameter(PARAM_COL_NAMES, String.class);
                    if (vals.size() < 1) {
                        return "Input parameter '" + PARAM_COL_NAMES + "' is empty.";
                    }
                } catch (Exception e) {
                    return "Input parameter '" + PARAM_COL_NAMES + "' is not of expected type 'string list'.";
                }
            }

            if (!aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
                return "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
            }
        }
        return msg;
    }

    void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        String msg = null;
        final String key = aConfig.getInputParameter(PARAM_INPUT_TABLE);
        if (key == null) {
            msg = "Input parameter at port 1 is missing!";
        } else if (!validateNamedRdd(key)) {
            msg = "Input data table missing!";
        }
        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * run the actual job, the result is serialized back to the client
     *
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.INFO, "starting job to convert nominal values...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final List<String> colNames = aConfig.getInputListParameter(PARAM_COL_NAMES, String.class);
        final List<Integer> colIdxs = SupervisedLearnerUtils.getSelectedColumnIds(aConfig);
        final int[] colIds = new int[colIdxs.size()];
        final Map<Integer, String> colNameForIndex = new HashMap<>();
        int i = 0;
        for (Integer ix : colIdxs) {
            colIds[i] = ix;
            colNameForIndex.put(ix, colNames.get(i));
            i++;
        }

        return execute(sc, aConfig, rowRDD, colIds, colNameForIndex);
    }

    abstract JobResult execute(final SparkContext aContext, final JobConfig aConfig, final JavaRDD<Row> aRowRDD,
        final int[] aColIds, final Map<Integer, String> aColNameForIndex) throws GenericKnimeSparkException;
}
