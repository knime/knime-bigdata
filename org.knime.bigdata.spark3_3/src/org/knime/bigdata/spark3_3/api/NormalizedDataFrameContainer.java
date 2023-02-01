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
 *   Created on 30.07.2015 by dwk
 */
package org.knime.bigdata.spark3_3.api;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * modifiable container for min/max values
 *
 * @author dwk
 */
@SparkClass
class MinMax implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * min values (modifiable!)
     */
    public final double[] min;

    /**
     * max values (modifiable!)
     */
    public final double[] max;

    MinMax(final int aLength) {
        min = new double[aLength];

        max = new double[aLength];

        Arrays.fill(min, Double.MAX_VALUE);
        Arrays.fill(max, Double.MIN_VALUE);
    }
}

/**
 * container for min/max values and RDD (transient!)
 *
 * @author dwk
 */
@SparkClass
public class NormalizedDataFrameContainer implements Serializable{

    private static final long serialVersionUID = 1L;

    private final double[] m_scale;

    private final double[] m_translation;

    /**
     * @param aScale
     * @param aTranslation
     */
    NormalizedDataFrameContainer(final double[] aScale, final double[] aTranslation) {
        m_scale = aScale;
        m_translation = aTranslation;
    }


    /**
     * normalize given value by multiplying it with pre-computed scale at this index and then adding the translation value
     *
     * @param aIndex
     * @param aValue
     * @return normalized value, minimum if range is very small
     */
    public double normalize(final int aIndex, final double aValue) {
        return m_scale[aIndex] * aValue + m_translation[aIndex];
    }

    /**
     * Normalize the given data frame, column indices must match the pre-set scale and translation array.
     *
     * @param spark
     * @param input
     * @param columnIndices
     * @return normalized data frame
     */
    public Dataset<Row> normalize(final SparkSession spark, final Dataset<Row> input, final Collection<Integer> columnIndices) {
        JavaRDD<Row> normalizedRdd = input.javaRDD().map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Row aRow) throws Exception {
                RowBuilder builder = RowBuilder.emptyRow();
                int ix = 0;
                for (int i = 0; i < aRow.length(); i++) {
                    if (columnIndices.contains(i)) {
                        Object val = aRow.get(i);
                        if (val == null) {
                            builder.add(null);
                        } else if (val instanceof Number) {
                            builder.add(normalize(ix, ((Number)val).doubleValue()));
                        } else {
                            builder.add(null);
                        }
                        ix++;
                    } else {
                        builder.add(aRow.get(i));
                    }
                }
                return builder.build();
            }
        });

        final StructField inputFields[] = input.schema().fields();
        final StructField outputFields[] = new StructField[inputFields.length];
        for (int i = 0; i < inputFields.length; i++) {
            if (columnIndices.contains(i)) {
                outputFields[i] = DataTypes.createStructField(inputFields[i].name(), DataTypes.DoubleType, true);
            } else {
                outputFields[i] = inputFields[i];
            }
        }
        return spark.createDataFrame(normalizedRdd, DataTypes.createStructType(outputFields));
    }

    /**
     * @return copy of scales
     */
    public double[] getScales() {
        return Arrays.copyOf(m_scale, m_scale.length);
    }

    /**
     * @return copy of scales
     */
    public double[] getTranslations() {
        return Arrays.copyOf(m_translation, m_translation.length);
    }

}