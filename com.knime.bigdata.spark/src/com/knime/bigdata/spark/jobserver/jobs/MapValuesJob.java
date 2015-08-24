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
 *   Created on 21.08.2015 by koetter
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.ColumnBasedValueMapping;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MapValuesJob extends KnimeSparkJob {

    private final static Logger LOGGER = Logger.getLogger(FetchRowsJob.class.getName());

    /**The parameter that contains the {@link ColumnBasedValueMapping}. */
    public static final String PARAM_MAPPING = "mapping";

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;

        if (!aConfig.hasInputParameter(PARAM_MAPPING)) {
            msg = "Input parameter '" + PARAM_MAPPING + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JobResult runJobWithContext(final SparkContext aSparkContext, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        LOGGER.log(Level.INFO, "Start column mapping job");
        if (!validateNamedRdd(aConfig.getInputParameter(PARAM_INPUT_TABLE))) {
            throw new GenericKnimeSparkException("Input data table missing for key: "+aConfig.getInputParameter(PARAM_INPUT_TABLE));
        }
        LOGGER.log(Level.INFO, "In map values job");
        final JavaRDD<Row> inputRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final ColumnBasedValueMapping m_map = aConfig.decodeFromInputParameter(PARAM_MAPPING);
        final Function<Row, Row> function = new Function<Row, Row>(){
            private static final long serialVersionUID = 1L;
//            private transient ColumnBasedValueMapping m_map = null;
            @Override
            public Row call(final Row r) throws Exception {
//                if (m_map == null) {
//                    m_map = aConfig.decodeFromInputParameter(PARAM_MAPPING);
//                }
                final RowBuilder rowBuilder = RowBuilder.fromRow(r);
                final List<Integer> idxs = m_map.getColumnIndices();
                for (final Integer idx : idxs) {
                    final Object object = r.get(idx);
                    final Object mapVal = m_map.map(idx, object);
                    rowBuilder.add(mapVal);
                }
                return rowBuilder.build();
            }
        };
        final JavaRDD<Row> mappedRDD = inputRDD.map(function);
        LOGGER.log(Level.INFO, "Mapping done");
        addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), mappedRDD);
        return JobResult.emptyJobResult().withMessage("OK")
                .withTable(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), null);
    }

}
