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
package com.knime.bigdata.spark1_5.jobs.pmml;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.pmml.predictor.PMMLPredictionJobInput;
import com.knime.bigdata.spark1_5.base.RowBuilder;

/**
 * applies a compiled pmml model to the input data
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PMMLPredictionJob extends PMMLAssignJob<PMMLPredictionJobInput> {
    private static final Logger LOGGER = Logger.getLogger(PMMLPredictionJob.class.getName());

    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    protected Function<Row, Row> createFunction(final Map<String, byte[]> bytecode, final String mainClass,
        final List<Integer> inputColIdxs, final PMMLPredictionJobInput input) {
        LOGGER.debug("Create pmml prediction function");
        final Function<Row, Row> predict = new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;
            final boolean addProbabilites = input.appendProbabilities();
            //use transient since a Method can not be serialized
            private transient Method m_evalMethod;
            /** {@inheritDoc} */
            @Override
            public Row call(final Row r) throws Exception {
                if (m_evalMethod == null) {
                    final ClassLoader cl = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
                        /** {@inheritDoc} */
                        @Override
                        protected Class<?> findClass(final String name) throws ClassNotFoundException {
                            byte[] bc = bytecode.get(name);
                            return defineClass(name, bc, 0, bc.length);
                        }
                    };
                    final Class<?> modelClass = cl.loadClass(mainClass);
                    m_evalMethod = modelClass.getMethod("evaluate", Object[].class);
                }
                final Object[] in = new Object[inputColIdxs.size()];
                for (int i = 0; i < inputColIdxs.size(); i++) {
                    final Integer colIdx = inputColIdxs.get(i);
                    if (colIdx == null || colIdx < 0) {
                        in[i] = null;
                    } else {
                        in[i] = r.get(colIdx);
                    }
                }
                final Object[] result = (Object[])m_evalMethod.invoke(null, (Object)in);

                final RowBuilder rowBuilder = RowBuilder.fromRow(r);
                //this is a PMML prediction task
                if (addProbabilites) {
                    return rowBuilder.addAll(Arrays.asList(result)).build();
                }
                return rowBuilder.add(result[0]).build();
            }
        };
        return predict;
    }
}
