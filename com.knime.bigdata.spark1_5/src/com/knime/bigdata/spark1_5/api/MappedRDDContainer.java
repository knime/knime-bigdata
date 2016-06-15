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
 *   Created on 17.07.2015 by dwk
 */
package com.knime.bigdata.spark1_5.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import com.knime.bigdata.spark.node.preproc.convert.MyRecord;
import com.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;

/**
 *
 * @author dwk
 */
@SparkClass
public class MappedRDDContainer implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * mapped data (original + converted data)
     */
    public transient final JavaRDD<Row> m_RddWithConvertedValues;

    /**
     * the mappings of nominal values to numbers
     */
    public final NominalValueMapping m_Mappings;

    private Map<Integer, String> m_colNames;

    /**
     * @param aRddWithConvertedValues
     * @param aMappings
     */
    public MappedRDDContainer(final JavaRDD<Row> aRddWithConvertedValues, final NominalValueMapping aMappings) {
        if (aMappings == null) {
            throw new NullPointerException("aMappings must not be null!");
        }
        m_RddWithConvertedValues = aRddWithConvertedValues;
        m_Mappings = aMappings;
    }

    /**
     * @param aColNames
     */
    private void setColumnNames(final Map<Integer, String> aColNames) {
        m_colNames = Collections.unmodifiableMap(aColNames);
    }

    /**
     *
     * @return map with column names and their corresponding indices, includes original columns and mapped columns, does
     *         not include columns that were not converted
     */
    public Map<Integer, String> getColumnNames() {
        return m_colNames;
    }

    /**
     * extract a list of rows with mapping values from the mapped data, as a side effect, sets the column names in
     * container
     *
     * @param aColNameForIndex
     * @param aOffset
     * @return list of rows with mapping values
     */
    public List<Row> createMappingTable(final Map<Integer, String> aColNameForIndex, int aOffset) {
        final Map<Integer, String> colNames = new LinkedHashMap<>(aColNameForIndex);
        final Iterator<MyRecord> iter = m_Mappings.iterator();
        final List<Row> rows = new ArrayList<>();
        String lastSeenName = null;
        while (iter.hasNext()) {
            MyRecord record = iter.next();
            final String colName = aColNameForIndex.get(record.m_nominalColumnIndex);
            RowBuilder builder = RowBuilder.emptyRow();
            final String name;
            if (m_Mappings.getType() == MappingType.BINARY) {
                name = colName + "_" + record.m_nominalValue;
                colNames.put(aOffset++, name);
            } else {
                name = colName + NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;
                if (!name.equals(lastSeenName)) {
                    colNames.put(aOffset++, name);
                }
                lastSeenName = name;
            }
            builder.add(name);
            builder.add(record.m_nominalColumnIndex).add(record.m_nominalValue).add(record.m_numberValue);
            rows.add(builder.build());

            builder = RowBuilder.emptyRow();
            builder.add(colName);
            builder.add(record.m_nominalColumnIndex).add(record.m_nominalValue).add(record.m_numberValue);
            rows.add(builder.build());
        }

        setColumnNames(colNames);
        return rows;
    }
}
