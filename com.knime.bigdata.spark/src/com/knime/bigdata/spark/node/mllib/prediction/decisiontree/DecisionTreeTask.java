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
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

/**
 *
 * @author koetter
 */
public class DecisionTreeTask implements Serializable {

    private static final long serialVersionUID = 1L;
    private String m_query;
    private Collection<Integer> m_numericColIdx;
    private int m_classColIdx;
    private String m_classColName;

    public DecisionTreeTask(final String sql, final Collection<Integer> numericColIdx, final String classColName,
        final int classColIdx) {
        m_query = sql;
        m_numericColIdx = numericColIdx;
        m_classColName = classColName;
        m_classColIdx = classColIdx;
    }

    public DecisionTreeModel execute(final JavaHiveContext sqlsc) {
        final JavaSchemaRDD uniqeClassRDD =
                sqlsc.sql("SELECT distinct(t." + m_classColName + ") FROM (" + m_query + ") as t");
        final List<Row> classValRows = uniqeClassRDD.collect();
        final Map<String, Double> classValMap = new HashMap<>();
        for (Row row : classValRows) {
            final String val = row.get(0).toString();
            if (!classValMap.containsKey(val)) {
                classValMap.put(val, Double.valueOf(classValMap.size()));
            }
        }
        final JavaSchemaRDD inputData = sqlsc.sql(m_query);
        final Function<Row, LabeledPoint> rowFunction = new Function<Row, LabeledPoint>() {
            private static final long serialVersionUID = 1L;
            @Override
            public LabeledPoint call(final Row r) {
                final double[] vals = new double[m_numericColIdx.size()];
                int colCount = 0;
                for (Integer id : m_numericColIdx) {
                    vals[colCount++] = r.getDouble(id.intValue());
                }
                final String classVal = r.get(m_classColIdx).toString();
                return new LabeledPoint(classValMap.get(classVal).doubleValue(), Vectors.dense(vals));
            }
        };
        final JavaRDD<LabeledPoint> parsedData = inputData.map(rowFunction);
        parsedData.cache();
        List<LabeledPoint> rows = parsedData.collect();
        for (LabeledPoint row : rows) {
            System.out.println(row);
        }
     // Cluster the data into two classes using KMeans
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        String impurity = "variance";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        final DecisionTreeModel model =
                DecisionTree.trainRegressor(parsedData, categoricalFeaturesInfo, impurity, maxDepth, maxBins);
        return model;
    }

}
