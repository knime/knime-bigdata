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
package com.knime.bigdata.spark.node.mllib.pmml.predictor;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import org.knime.base.pmml.translation.java.compile.CompiledModel;

/**
 *
 * @author koetter
 */
public class PredictionTask implements Serializable {

    private String m_resultTableName;
    private String m_query;
    private SerializableCompiledModel m_model;
    private int[] m_inputMapping;
    private int[] m_copiedCols;

    /**
     *
     * @param query
     * @param resultTableName
     * @param model
     * @param inputMapping
     * @param outputMapping
     */
    public PredictionTask(final String query, final String resultTableName,
                        final SerializableCompiledModel model, final int[] inputMapping, final int[] copiedCols) {
        m_query = query;
        m_resultTableName = resultTableName;
        m_model = model;
        m_inputMapping = inputMapping;
        m_copiedCols = copiedCols;
    }

    public void execute(final JavaHiveContext sqlsc, final StructType resultSchema) {

        final JavaSchemaRDD inputData = sqlsc.sql(m_query);

        final Function<Row, Row> rowFunction = new Function<Row, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Row r) {
                final CompiledModel model = m_model.getCompiledModel();
                Object[] in = new Object[m_inputMapping.length];
                for (int i = 0; i < in.length; i++) {
                    int index = m_inputMapping[i];
                    in[index] = r.get(i);
                }
                Object[] result = null;
                try {
                    result = model.evaluate(in);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (result != null) {
                    if (m_copiedCols.length == 0) {
                        return Row.create(result);
                    } else {
                        Object[] output = new Object[result.length + m_copiedCols.length];
                        for (int i = 0; i < m_copiedCols.length; i++) {
                            output[i] = r.get(m_copiedCols[i]);
                        }
                        for (int i = 0; i < result.length; i++) {
                            output[i + m_copiedCols.length] = result[i];
                        }
                        return Row.create(output);
                    }
                }
                return null;
            }
        };

        final JavaRDD<Row> predicted = inputData.map(rowFunction);

        final JavaSchemaRDD schemaPredictedData = sqlsc.applySchema(predicted, resultSchema);
        schemaPredictedData.saveAsTable(m_resultTableName);
    }

}
