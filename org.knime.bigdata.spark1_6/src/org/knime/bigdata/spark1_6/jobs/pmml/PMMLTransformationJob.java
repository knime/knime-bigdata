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
package org.knime.bigdata.spark1_6.jobs.pmml;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.pmml.transformation.PMMLTransformationJobInput;
import org.knime.bigdata.spark1_6.api.RowBuilder;

/**
 * applies a compiled pmml model to the input data
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PMMLTransformationJob extends PMMLAssignJob<PMMLTransformationJobInput> {

    private static final Logger LOGGER = Logger.getLogger(PMMLTransformationJob.class.getName());
    /**
     * {@inheritDoc}
     */
    @Override
    protected Function<Row, Row> createFunction(final Map<String, byte[]> bytecode, final String mainClass, final List<Integer> inputColIdxs,
        final PMMLTransformationJobInput input) {
        LOGGER.debug("Create pmml transformation function");
        final boolean replace = input.replace();
        final List<Integer> resultColIdxs2Add = input.getAdditionalColIdxs();
        final List<Integer> tmp = input.getSkippedColIdxs();
        final Set<Integer> inputColIdxs2Skip = new HashSet<>(tmp);
        final Function<Row, Row> predict = new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

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

                final RowBuilder rowBuilder;
                if (replace) {
                    rowBuilder = RowBuilder.emptyRow();
                    for (int i = 0; i < r.length(); i++) {
                        if (inputColIdxs2Skip.contains(Integer.valueOf(i))) {
                            //skip the input
                            continue;
                        }
                        rowBuilder.add(r.get(i));
                    }
                } else {
                    rowBuilder = RowBuilder.fromRow(r);
                }
                //this is a PMML transformation task
                for (int i = 0; i < resultColIdxs2Add.size(); i++) {
                    final Integer colIdx = resultColIdxs2Add.get(i);
                    rowBuilder.add(result[colIdx]);
                }
                return rowBuilder.build();
            }
        };
        return predict;
    }
}
