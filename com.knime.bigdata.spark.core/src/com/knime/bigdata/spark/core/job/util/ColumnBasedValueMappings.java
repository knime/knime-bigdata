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
 *   Created on May 30, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.job.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.xmlbeans.XmlObject;
import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.dmg.pmml.InlineTableDocument.InlineTable;
import org.dmg.pmml.MapValuesDocument.MapValues;
import org.dmg.pmml.RowDocument.Row;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;

/**
 * This class provides load and save methods for {@link ColumnBasedValueMapping}. The reason why these methods are not
 * in {@link ColumnBasedValueMapping} itself, is that {@link ColumnBasedValueMapping} is a {@link SparkClass}, but we
 * also need to be able to also load {@link ColumnBasedValueMapping}s from KNIME specific sources, such as
 * {@link PMMLPortObject}s.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class ColumnBasedValueMappings {

    /**
     *
     * @param inputStream
     * @return a {@link ColumnBasedValueMapping} which has been loaded from the given input stream, or null if no
     *         mappings were saved.
     * @throws IOException
     */
    public static ColumnBasedValueMapping load(final InputStream inputStream) throws IOException {
        try (ObjectInputStream in = new ObjectInputStream(inputStream)) {

            @SuppressWarnings("unchecked")
            final Map<Integer, Map<Serializable, Serializable>> mappings =
                (Map<Integer, Map<Serializable, Serializable>>)in.readObject();

            if (mappings == null) {
                return null;
            }

            final ColumnBasedValueMapping mapping = new ColumnBasedValueMapping();

            for (Integer columnIndex : mappings.keySet()) {
                final Map<Serializable, Serializable> columnMap = mappings.get(columnIndex);
                for (Serializable columnMappingInput : columnMap.keySet()) {
                    mapping.add(columnIndex, columnMappingInput, columnMap.get(columnMappingInput));
                }
            }

            return mapping;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    /**
     * Saves the given mapping to the given output stream.
     *
     * @param outputStream
     * @param mapping the mapping to save, may be null
     * @throws IOException
     */
    public static void save(final OutputStream outputStream, final ColumnBasedValueMapping mapping) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(outputStream)) {
            if (mapping != null) {
                out.writeObject(mapping.getAllMappings());
            } else {
                out.writeObject(null);
            }
        }
    }

    /**
     * @param pmml {@link PMMLPortObject} that contains the category to number mapping
     * @param rdd the {@link SparkDataPortObject} to map
     * @return the {@link ColumnBasedValueMapping} class with the mapping
     */
    public static ColumnBasedValueMapping fromPMMLPortObject(final PMMLPortObject pmml, final SparkDataPortObject rdd) {
        final DataTableSpec tableSpec = rdd.getTableSpec();
        final ColumnBasedValueMapping map = new ColumnBasedValueMapping();
        final DerivedField[] fields = pmml.getDerivedFields();
        if (fields != null) {
            for (final DerivedField field : fields) {
                final MapValues mapValues = field.getMapValues();
                if (mapValues != null) {
                    final String in = mapValues.getFieldColumnPairList().get(0).getColumn();
                    final String out = mapValues.getOutputColumn();
                    final int colIdx = tableSpec.findColumnIndex(field.getName());
                    if (colIdx >= 0) {
                        final InlineTable table = mapValues.getInlineTable();
                        for (final Row row : table.getRowList()) {
                            final XmlObject[] inChilds = row.selectChildren("http://www.dmg.org/PMML-4_0", in);
                            final String inVal = inChilds.length > 0 ? inChilds[0].newCursor().getTextValue() : null;
                            final XmlObject[] outChilds = row.selectChildren("http://www.dmg.org/PMML-4_0", out);
                            final String outVal = outChilds.length > 0 ? outChilds[0].newCursor().getTextValue() : null;
                            if (null == inVal || null == outVal) {
                                throw new IllegalArgumentException(
                                    "The PMML model" + "is not complete. Missing element in InlineTable.");
                            }
                            map.add(colIdx, Double.parseDouble(outVal), inVal);
                        }
                    }
                }
            }
        }
        return map;
    }
}
