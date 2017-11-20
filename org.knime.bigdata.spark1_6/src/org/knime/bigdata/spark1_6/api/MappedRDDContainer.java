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
package org.knime.bigdata.spark1_6.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import org.knime.bigdata.spark.node.preproc.convert.MyRecord;
import org.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;

/**
 * @author dwk
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class MappedRDDContainer implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * mapped data (original + converted data)
     */
    private transient final JavaRDD<Row> m_rddWithConvertedValues;

    /**
     * the mappings of nominal values to numbers
     */
    private final NominalValueMapping m_mappings;

    /**
     * Appended column names
     */
    private final String m_appendedColNames[];

    /**
     * @param rddWithConvertedValues - rdd with mapped values
     * @param mappings - mapping of this container
     * @param appendedColumns - appended column names
     */
    public MappedRDDContainer(final JavaRDD<Row> rddWithConvertedValues, final NominalValueMapping mappings,
            final String appendedColumns[]) {

        m_rddWithConvertedValues = rddWithConvertedValues;
        m_mappings = mappings;
        m_appendedColNames = appendedColumns;
    }

    /**
     * @return the rddWithConvertedValues
     */
    public JavaRDD<Row> getRddWithConvertedValues() {
        return m_rddWithConvertedValues;
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
     * @param mappedRdd - rdd with mapped data
     * @param columnIds - included (mapped) column indices
     * @param columnNames - included (mapped) column names
     * @param mappings - mapping of this container
     * @param keepOriginalColumns - keep original columns or not
     * @return new container
     */
    public static MappedRDDContainer createContainer(final JavaRDD<Row> mappedRdd,
            final int[] columnIds, final String[] columnNames,
            final NominalValueMapping mappings, final boolean keepOriginalColumns) {

        List<String> appendedColumns = new ArrayList<>();

        if (mappings.getType() == MappingType.BINARY) {
            // ordered by column index and mapped value index:
            final Iterator<MyRecord> mappingIter = mappings.iterator();
            MyRecord currentMapping = null;
            for (int i=0; i < columnIds.length; i++) {
                if (mappings.hasMappingForColumn(columnIds[i])) {
                    int numValues = mappings.getNumberOfValues(columnIds[i]);
                    for (int j = 0; j < numValues; j++) {
                        currentMapping = mappingIter.next();
                        String name = columnNames[i] + "_" + currentMapping.m_nominalValue;
                        appendedColumns.add(name);
                    }
                }
            }

        } else {
            for (int i=0; i < columnIds.length; i++) {
                if (mappings.hasMappingForColumn(columnIds[i])) {
                    String name = columnNames[i] + NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;
                    appendedColumns.add(name);
                }
            }
        }

        return new MappedRDDContainer(mappedRdd, mappings, appendedColumns.toArray(new String[0]));
    }
}
