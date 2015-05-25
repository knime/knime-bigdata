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
package com.knime.bigdata.spark.node.mllib.clustering.kmeans;

import java.io.Serializable;
import java.util.Collection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
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
public class KMeansWithHiveTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private String m_query;

    private Collection<Integer> m_numericColIdx;

    private String m_tableName;

    private int m_noOfIteration;

    private int m_noOfCluster;

    public KMeansWithHiveTask(final String sql, final Collection<Integer> numericColIdx,
        final String tableName, final int noOfCluster, final int noOfIteration) {
        m_query = sql;
        m_numericColIdx = numericColIdx;
        m_tableName = tableName;
        m_noOfCluster = noOfCluster;
        m_noOfIteration = noOfIteration;
    }

    public KMeansModel execute(final JavaHiveContext sqlsc, final StructType resultSchema) {
        final JavaSchemaRDD inputData = sqlsc.sql(m_query);
        inputData.cache();
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
        final KMeansModel kMeansModel = KMeans.train(parsedData.rdd(), m_noOfCluster, m_noOfIteration);

        final JavaRDD<Row> predictedData = inputData.map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Row v) {
                final double[] doubleVals = new double[m_numericColIdx.size()];
                final Object[] vals = new Object[v.length() + 1];
                int valCount = 0;
                int doubleColCount = 0;
                for (int idx = 0; idx < v.length(); idx++) {
                    if (m_numericColIdx.contains(Integer.valueOf(idx))) {
                        final double dVal = v.getDouble(idx);
                        doubleVals[doubleColCount++] = dVal;
                        vals[valCount++] = Double.valueOf(dVal);
                    } else {
                        vals[valCount++] = v.getString(idx);
                    }
                }
                final int cluster = kMeansModel.predict(Vectors.dense(doubleVals));
                vals[valCount++] = "cluster_" + cluster;
                return Row.create(vals);
            }
        });
        final JavaSchemaRDD schemaPredictedData = sqlsc.applySchema(predictedData, resultSchema);
        schemaPredictedData.saveAsTable(m_tableName);
        return kMeansModel;
    }

}
