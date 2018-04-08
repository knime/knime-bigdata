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
package org.knime.bigdata.spark1_2.jobs.pmml;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.pmml.predictor.PMMLPredictionJobInput;
import org.knime.bigdata.spark1_2.api.RowBuilder;

/**
 * Job that applies a compiled PMML prediction model to the input data.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PMMLPredictionJob extends PMMLAssignJob<PMMLPredictionJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PMMLPredictionJob.class.getName());

    @Override
    protected Function<Row, Row> createMapFunction(final Map<String, byte[]> bytecode, final PMMLPredictionJobInput input) {

        final String mainClass = input.getMainClass();
        final ArrayList<Integer> inputColIdxs = new ArrayList<>(input.getColumnIdxs());
        final boolean[] longColumns = input.getInputLongFields();

        LOGGER.debug("Creating PMML prediction function");

        return new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;
            final boolean addProbabilites = input.appendProbabilities();

            // use transient since a Method can not be serialized
            private transient Method m_evalMethod;

            // use transient because we have to initialize this array per task anyway
            private transient Object[] m_evalMethodInput;

            @Override
            public Row call(final Row r) throws Exception {
                if (m_evalMethod == null) {
                    m_evalMethod = loadCompiledPMMLEvalMethod(bytecode, mainClass);
                    m_evalMethodInput = new Object[inputColIdxs.size()];
                }

                fillEvalMethodInputFromRow(inputColIdxs, r, m_evalMethodInput, longColumns);
                final Object[] result = (Object[])m_evalMethod.invoke(null, (Object)m_evalMethodInput);

                final RowBuilder rowBuilder = RowBuilder.fromRow(r);

                // this is a PMML prediction task
                if (addProbabilites) {
                    return rowBuilder.addAll(Arrays.asList(result)).build();
                } else {
                    return rowBuilder.add(result[0]).build();
                }
            }
        };
    }
}
