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
 *   Created on 21.07.2015 by koetter
 */
package com.knime.bigdata.spark.util;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.dmg.pmml.InlineTableDocument.InlineTable;
import org.dmg.pmml.MapValuesDocument.MapValues;
import org.knime.base.pmml.translation.CompiledModel;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.SparkPlugin;
import com.knime.bigdata.spark.jobserver.server.NominalFeatureInfo;
import com.knime.bigdata.spark.util.converter.SparkTypeConverter;
import com.knime.bigdata.spark.util.converter.SparkTypeRegistry;

/**
 *
 * @author koetter
 */
public final class SparkUtil {

    /**
     * Prevent object creation.
     */
    private SparkUtil() {}

    /**
     * @param tableSpec the {@link DataTableSpec}
     * @param colNames the column names to get the indices for
     * @return the indices of the columns in the same order as in the input list
     * @throws InvalidSettingsException if the input list is empty or a column name could not be found in the input spec
     */
    public static Integer[] getColumnIndices(final DataTableSpec tableSpec, final List<String> colNames)
        throws InvalidSettingsException {
        if (colNames == null || colNames.isEmpty()) {
            throw new InvalidSettingsException("No columns selected");
        }
        final Integer[] numericColIdx = new Integer[colNames.size()];
        int idx = 0;
        for (String numericColName : colNames) {
            final int colIdx = tableSpec.findColumnIndex(numericColName);
            if (colIdx < 0) {
                throw new InvalidSettingsException("Column: " + numericColName + " not found in input data");
            }
            numericColIdx[idx++] = Integer.valueOf(colIdx);
        }
        return numericColIdx;
    }

    /**
     * @param tableSpec the {@link DataTableSpec}
     * @param featureColNames the column names to get the indices for
     * @return the indices of the columns in the same order as in the input list
     * @throws InvalidSettingsException if the input list is empty or a column name could not be found in the input spec
     */
    public static Integer[] getColumnIndices(final DataTableSpec tableSpec, final String... featureColNames)
        throws InvalidSettingsException {
        if (featureColNames == null || featureColNames.length < 1) {
            throw new InvalidSettingsException("No columns selected");
        }
        final Integer[] colIdxs = new Integer[featureColNames.length];
        for (int i = 0, length = featureColNames.length; i < length; i++) {
            final String colName = featureColNames[i];
            final int colIdx = tableSpec.findColumnIndex(colName);
            if (colIdx < 0) {
                throw new InvalidSettingsException("Column: " + colName + " not found in input data");
            }
            colIdxs[i] = colIdx;
        }
        return colIdxs;
    }

    /**
     * @return the path to the standard KNIME job jar
     */
    public static String getJobJarPath() {
        return SparkPlugin.getDefault().getPluginRootPath() + File.separatorChar + "resources" + File.separatorChar
            + "knimeJobs.jar";
    }

    /**
     * @param inputSpec {@link DataTableSpec}
     * @param model PMML {@link CompiledModel}
     * @return the indices of the columns required by the compiled PMML model
     * @throws InvalidSettingsException if a required column is not present in the input table
     */
    public static Integer[] getColumnIndices(final DataTableSpec inputSpec, final CompiledModel model)
            throws InvalidSettingsException {
        final String[] inputFields = model.getInputFields();
        final Integer[] colIdxs = new Integer[inputFields.length];
        for (String fieldName : inputFields) {
            final int colIdx = inputSpec.findColumnIndex(fieldName);
            if (colIdx < 0) {
                throw new InvalidSettingsException("Column with name " + fieldName + " not found in input data");
            }
            colIdxs[model.getInputFieldIndex(fieldName)] = Integer.valueOf(colIdx);
        }
        return colIdxs;
    }

    /**
     * @param spec {@link DataTableSpec} to convert
     * @return the {@link StructType} representing the input {@link DataTableSpec}
     */
    public static StructType toStructType(final DataTableSpec spec) {
        final List<StructField> structFields = new ArrayList<>(spec.getNumColumns());
        for (final DataColumnSpec colSpec : spec) {
            final SparkTypeConverter<?, ?> converter = SparkTypeRegistry.get(colSpec.getType());
            final StructField field = DataType.createStructField(colSpec.getName(), converter.getSparkSqlType(), true);
            structFields.add(field);
        }
        final StructType schema = DataType.createStructType(structFields);
        return schema;
    }

    /**
     * @param schema {@link StructType} that describes the columns
     * @return the corresponding {@link DataTableSpec}
     */
    public static DataTableSpec toTableSpec(final StructType schema) {
        final List<DataColumnSpec> specs = new LinkedList<>();
        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("Test", StringCell.TYPE);
        for (final StructField field : schema.getFields()) {
            specCreator.setName(field.getName());
            final SparkTypeConverter<?, ?> typeConverter = SparkTypeRegistry.get(field.getDataType());
            specCreator.setType(typeConverter.getKNIMEType());
            specs.add(specCreator.createSpec());
        }
        return new DataTableSpec(specs.toArray(new DataColumnSpec[0]));
    }

    /**
     * @param featureColNames the names of the feature columns in the same order as in the Spark RDD
     * @param mapValues the derived fields from the PMML
     * @return the nominal column indices as first element and the number of unique values of the corresponding
     * column as second argument
     */
    public static NominalFeatureInfo getNominalFeatureInfo(final List<String> featureColNames,
        final Map<String, DerivedField> mapValues) {
        final NominalFeatureInfo nominalFeatureInfo = new NominalFeatureInfo();
        int idx = 0;
        for (final String col : featureColNames) {
            final DerivedField derivedField = mapValues.get(col);
            if (derivedField != null) {
                final MapValues map = derivedField.getMapValues();
                final InlineTable table = map.getInlineTable();
                final int noOfVals = table.sizeOfRowArray();
                nominalFeatureInfo.add(Integer.valueOf(idx), noOfVals);
            }
            idx++;
        }
        return nominalFeatureInfo;
    }

    /**
     * @param mapValues the {@link DerivedField}
     * @param classColName the name of the class column
     * @return the number of unique classes if the class column is one of the {@link DerivedField}s
     */
    public static Long getNoOfClassVals(final Map<String, DerivedField> mapValues, final String classColName) {
        if (mapValues.containsKey(classColName)) {
            final DerivedField derivedField = mapValues.get(classColName);
            final MapValues map = derivedField.getMapValues();
            final InlineTable table = map.getInlineTable();
            final int noOfVals = table.sizeOfRowArray();
            return Long.valueOf(noOfVals);
        }
        return null;
    }

    /**
     * @param model the PMML model
     * @return the field in the first FieldColumnPair of the MapValues mapped
     * to the MapValues Model
     */
    public static Map<String, DerivedField> getMapValues(final PMMLPortObject model) {
        final Map<String, DerivedField> mapValues = new LinkedHashMap<String, DerivedField>();
        final DerivedField[] derivedFields = model.getDerivedFields();
        for (final DerivedField derivedField : derivedFields) {
            final MapValues map = derivedField.getMapValues();
            if (null != map) {
                // This is the field name the mapValues is based on
                String name = derivedField.getDisplayName();
                if (name == null) {
                    name = derivedField.getName();
                }
                mapValues.put(name, derivedField);
            }
        }
        return mapValues;
    }
}
