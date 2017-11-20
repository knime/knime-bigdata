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
 *   Created on 17.07.2015 by dwk
 */
package org.knime.bigdata.spark2_0.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import org.knime.bigdata.spark.node.preproc.convert.MyRecord;
import org.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;

/**
 *
 * @author dwk
 */
@SparkClass
public class MappedDatasetContainer implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * mapped data (original + converted data)
     */
    private transient final Dataset<Row> m_datasetWithConvertedValues;

    /**
     * the mappings of nominal values to numbers
     */
    private final NominalValueMapping m_mappings;

    /**
     * Appended column names
     */
    private final String m_appendedColNames[];

    /**
     * @param datasetWithConvertedValues - mapped dataset
     * @param mappings - mapping of this container
     * @param appendedColumns - appended column names
     */
    public MappedDatasetContainer(final Dataset<Row> datasetWithConvertedValues,
            final NominalValueMapping mappings, final String appendedColumns[]) {

        m_datasetWithConvertedValues = datasetWithConvertedValues;
        m_mappings = mappings;
        m_appendedColNames = appendedColumns;
    }

    /**
     * @return the datasetWithConvertedValues
     */
    public Dataset<Row> getDatasetWithConvertedValues() {
        return m_datasetWithConvertedValues;
    }

    /**
     * @return the mappings
     */
    public NominalValueMapping getMappings() {
        return m_mappings;
    }

    /**
     * @return appended column names
     */
    public String[] getAppendedColumnNames() {
        return m_appendedColNames;
    }

    /**
     * Creates a new container and generates appended column names array.
     *
     * @param dataset - original dataset
     * @param mappedRdd - rdd with mapped data
     * @param columnIds - included (mapped) column indices
     * @param mappings - mapping of this container
     * @param keepOriginalColumns - keep original columns or not
     * @return new container
     */
    public static MappedDatasetContainer createContainer(final Dataset<Row> dataset, final JavaRDD<Row> mappedRdd,
            final int[] columnIds, final NominalValueMapping mappings, final boolean keepOriginalColumns) {

        SparkSession spark = SparkSession.builder().getOrCreate();
        List<Integer> columnIdsList = Arrays.asList(ArrayUtils.toObject(columnIds));
        Collections.sort(columnIdsList);
        List<StructField> fields = new ArrayList<>();
        List<String> appendedColumns = new ArrayList<>();
        StructField oldFields[] = dataset.schema().fields();

        for (int i = 0; i < oldFields.length; i++) {
            if (keepOriginalColumns || !columnIdsList.contains(i)) {
                fields.add(oldFields[i]);
            }
        }

        if (mappings.getType() == MappingType.BINARY) {
            // ordered by column index and mapped value index:
            final Iterator<MyRecord> mappingIter = mappings.iterator();
            MyRecord currentMapping = null;
            for (int i = 0; i < oldFields.length; i++) {
                if (mappings.hasMappingForColumn(i)) {
                    int numValues = mappings.getNumberOfValues(i);
                    for (int j = 0; j < numValues; j++) {
                        currentMapping = mappingIter.next();
                        String name = oldFields[i].name() + "_" + currentMapping.m_nominalValue;
                        fields.add(DataTypes.createStructField(name, DataTypes.DoubleType, false));
                        appendedColumns.add(name);
                    }
                }
            }

        } else {
            for (int i = 0; i < oldFields.length; i++) {
                if (mappings.hasMappingForColumn(i)) {
                    String name = oldFields[i].name() + NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;
                    fields.add(DataTypes.createStructField(name, DataTypes.DoubleType, false));
                    appendedColumns.add(name);
                }
            }
        }

        StructType outputSchema = DataTypes.createStructType(fields);
        Dataset<Row> mappedDataset = spark.createDataFrame(mappedRdd, outputSchema);
        return new MappedDatasetContainer(mappedDataset, mappings, appendedColumns.toArray(new String[0]));
    }
}
