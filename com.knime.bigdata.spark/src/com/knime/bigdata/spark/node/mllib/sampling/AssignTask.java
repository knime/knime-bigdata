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
package com.knime.bigdata.spark.node.mllib.sampling;

import java.io.Serializable;
import java.util.Collection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

/**
 *
 * @author koetter
 */
public class AssignTask implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private String m_resultTableName;
    private String m_query;
    private Collection<Integer> m_numericColIdx;

    /**
     * @param query
     * @param numericColIdx
     * @param resultTableName
     */
    public AssignTask(final String query, final Collection<Integer> numericColIdx, final String resultTableName) {
        m_query = query;
        m_numericColIdx = numericColIdx;
        m_resultTableName = resultTableName;
    }

    /**
     * @param sqlsc
     * @param resultSchema
     * @param kMeansModel
     */
    public void execute(final JavaHiveContext sqlsc, final StructType resultSchema,
        final KMeansModel kMeansModel) {
        final JavaSchemaRDD inputData = sqlsc.sql(m_query);
        final Function<Row, Vector> rowFunction = new Function<Row, Vector>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Vector call(final Row r) {
                final double[] vals = new double[m_numericColIdx.size()];
                int colCount = 0;
                for (Integer id : m_numericColIdx) {
                    vals[colCount++] = r.getDouble(id.intValue());
                }
                return Vectors.dense(vals);
            }
        };
        final JavaRDD<Vector> parsedData = inputData.map(rowFunction);
        parsedData.cache();
     // Cluster the data into two classes using KMeans
        final JavaRDD<Row> predictedData = parsedData.map(new Function<Vector, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Row call(final Vector v) {
                final int cluster = kMeansModel.predict(v);
                final Object[] vals = new Object[v.size() + 1];
                int valCount = 0;
                for (double d : v.toArray()) {
                    vals[valCount++] = Double.valueOf(d);
                }
                vals[valCount++] = Integer.valueOf(cluster);
                return Row.create(vals);
            }
        });
        final JavaSchemaRDD schemaPredictedData = sqlsc.applySchema(predictedData, resultSchema);
        schemaPredictedData.saveAsTable(m_resultTableName);
    }

}
