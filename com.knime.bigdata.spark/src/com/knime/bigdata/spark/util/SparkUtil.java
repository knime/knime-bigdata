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
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.apache.xmlbeans.XmlObject;
import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.dmg.pmml.InlineTableDocument.InlineTable;
import org.dmg.pmml.MapValuesDocument.MapValues;
import org.dmg.pmml.RowDocument.Row;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.SparkPlugin;
import com.knime.bigdata.spark.jobserver.server.ColumnBasedValueMapping;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.util.converter.SparkTypeConverter;
import com.knime.bigdata.spark.util.converter.SparkTypeRegistry;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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
     * @param colNames the column names to get the indices for
     * @return the indices of the columns in the same order as in the input list
     * @throws InvalidSettingsException if the input list is empty or a column name could not be found in the input spec
     */
    public static Integer[] getColumnIndices(final DataTableSpec tableSpec, final String... colNames)
        throws InvalidSettingsException {
        if (colNames == null || colNames.length < 1) {
            throw new InvalidSettingsException("No columns selected");
        }
        final Integer[] colIdxs = new Integer[colNames.length];
        for (int i = 0, length = colNames.length; i < length; i++) {
            final String colName = colNames[i];
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
     * @param pmml {@link PMMLPortObject} that contains the category to number mapping
     * @param rdd the {@link SparkDataPortObject} to map
     * @return the {@link ColumnBasedValueMapping} class with the mapping
     */
    public static ColumnBasedValueMapping getCategory2NumberMap(final PMMLPortObject pmml,
        final SparkDataPortObject rdd) {
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
                                throw new IllegalArgumentException("The PMML model"
                                    + "is not complete. Missing element in InlineTable.");
                            }
                            map.add(colIdx, Double.parseDouble(outVal), inVal);
                        }
                    }
                }
            }
        }
        return map;
    }

    /**
     * @param vals the primitive booleans to convert to Object array
     * @return the {@link Boolean} array representation
     */
    public static Boolean[] convert(final boolean[] vals) {
        return ArrayUtils.toObject(vals);
    }

    /**
     * Helper method that can be used for debugging the predictor params string that is send to the Spark job server
     * via the JobControler.startJobAndWaitForResult() method.
     * @param aJsonParams json formated string with job parameters
     * @throws GenericKnimeSparkException
     */
    public static void testPredictorParams(final String aJsonParams) throws GenericKnimeSparkException {
        Config conf = ConfigFactory.parseString(aJsonParams);
        JobConfig aConfig = new JobConfig(conf);
        @SuppressWarnings("unused")
        Object decodeFromInputParameter = aConfig.decodeFromInputParameter(ParameterConstants.PARAM_MODEL_NAME);
        aConfig.readInputFromFileAndDecode(ParameterConstants.PARAM_MODEL_NAME);
    }

    /**
     * @param vals the Integer vals to convert to int
     * @return the int array
     */
    public static int[] convert(final Integer[] vals) {
        return ArrayUtils.toPrimitive(vals);
    }
}
