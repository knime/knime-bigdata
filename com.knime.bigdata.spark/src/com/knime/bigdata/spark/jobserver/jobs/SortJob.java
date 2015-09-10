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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.MultiValueSortKey;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * sorts input RDD by given indices, in given order
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
public class SortJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(SortJob.class.getName());

    /**
     * indicates whether sort order is ascending or not
     */
    public static final String PARAM_SORT_IS_ASCENDING = "sortOrderAscending";

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = SupervisedLearnerUtils.checkSelectedColumnIdsParameter(aConfig);

        if (msg == null && !aConfig.hasInputParameter(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }

        if (!aConfig.hasInputParameter(PARAM_SORT_IS_ASCENDING) || (getSortOrders(aConfig).size() == 0)) {
            msg = "Input parameter '" + PARAM_SORT_IS_ASCENDING + "' missing.";
        }

        if (msg == null && !aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     *
     * @param aConfig
     * @return List of sort orders
     */
    static List<Boolean> getSortOrders(final JobConfig aConfig) {
        return aConfig.getInputListParameter(PARAM_SORT_IS_ASCENDING, Boolean.class);
    }

    private void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
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

    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.INFO, "starting RDD Sort job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final List<Integer> colIdxs = SupervisedLearnerUtils.getSelectedColumnIds(aConfig);
        final List<Boolean> sortOrders = getSortOrders(aConfig);

        final JavaRDD<Row> res = execute(sc.defaultMinPartitions(), rowRDD, colIdxs, sortOrders);
        addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), res);

        LOGGER.log(Level.INFO, "RDD Sort done");
        return JobResult.emptyJobResult().withMessage("OK");
    }

    static JavaRDD<Row> execute(final int aNumPartitions, final JavaRDD<Row> rowRDD, final List<Integer> colIdxs, final List<Boolean> sortOrders) {
        //special (and more efficient) handling of sorting by a single key:
        if (colIdxs.size() == 1) {
            return rowRDD.sortBy(new Function<Row, Object>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object call(final Row aRow) throws Exception {
                    return aRow.get(colIdxs.get(0));
                }
            }, sortOrders.get(0), aNumPartitions);
        } else {
            return rowRDD.sortBy(new Function<Row, MultiValueSortKey>() {
                private static final long serialVersionUID = 1L;

                @Override
                public MultiValueSortKey call(final Row aRow) throws Exception {
                    final Object[] values = new Object[colIdxs.size()];
                    final Boolean[] isAscending = new Boolean[sortOrders.size()];
                    for (int i=0; i<values.length; i++) {
                        values[i] = aRow.get(colIdxs.get(i));
                        isAscending[i] = sortOrders.get(i);
                    }
                    return new MultiValueSortKey(values, isAscending);
                }
            }, true, aNumPartitions);
        }
    }
}
