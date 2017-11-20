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
 *   Created on Nov 17, 2017 by bjoern
 */
package org.knime.bigdata.spark.node.preproc.groupby;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.knime.bigdata.spark.node.preproc.groupby.dialog.column.ColumnAggregationFunctionRow;
import org.knime.bigdata.spark.node.preproc.groupby.dialog.pattern.PatternAggregationFunctionRow;
import org.knime.bigdata.spark.node.preproc.groupby.dialog.type.DataTypeAggregationFunctionRow;
import org.knime.bigdata.spark.node.sql_function.SparkSQLAggregationFunction;
import org.knime.bigdata.spark.node.sql_function.SparkSQLFunctionCombinationProvider;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.database.aggregation.AggregationFunction;
import org.knime.core.node.port.database.aggregation.InvalidAggregationFunction;

/**
 * Holds settings that specify which aggregation function to use for which column.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @author Sascha Wolke, KNIME GmbH
 */
public class AggregationFunctionSettings {

    /**
     * Config key for the manual aggregation methods applied to the columns.
     */
    static final String CFG_MANUAL_AGGREGATION_FUNCTIONS = "manualAggregationFunctions";

    /**
     * Config key for the pattern based aggregation methods applied to the columns.
     */
    static final String CFG_PATTERN_AGGREGATION_FUNCTIONS = "patternAggregationFunctions";

    /**
     * Config key for the type based aggregation methods applied to the columns.
     */
    static final String CFG_TYPE_AGGREGATION_FUNCTIONS = "typeAggregationFunctions";

    private NodeSettings m_manualAggregationSettings = null;

    private NodeSettings m_patternAggregationSettings = null;

    private NodeSettings m_typeAggregationSettings = null;

    /**
     * Computes the list of aggregation functions to use per column.
     *
     * @param tableSpec Table spec of the input data.
     * @param functionProvider Used to look up Spark SQL aggregation functions.
     * @param groupByColumns The list of column names already used during grouping.
     * @param invalidColAggrs
     * @return the list of aggregation functions to use for each column.
     * @throws InvalidSettingsException If the settings for aggregation functions are invalid.
     */
    public List<ColumnAggregationFunctionRow> getAggregationFunctions(final DataTableSpec tableSpec,
        final SparkSQLFunctionCombinationProvider functionProvider, final Collection<String> groupByColumns,
        final List<ColumnAggregationFunctionRow> invalidColAggrs) throws InvalidSettingsException {

        final List<ColumnAggregationFunctionRow> m_aggregationFunction2Use = new LinkedList<>();
        final Set<String> usedColNames = new HashSet<>(groupByColumns);

        final List<ColumnAggregationFunctionRow> columnFunctions =
            ColumnAggregationFunctionRow.loadFunctions(m_manualAggregationSettings, functionProvider, tableSpec);


        for (ColumnAggregationFunctionRow row : columnFunctions) {
            final DataColumnSpec columnSpec = row.getColumnSpec();
            final DataColumnSpec inputSpec = tableSpec.getColumnSpec(columnSpec.getName());
            final AggregationFunction function = row.getFunction();
            if (inputSpec == null || !inputSpec.getType().equals(columnSpec.getType())) {
                invalidColAggrs.add(row);
                continue;
            }
            if (function instanceof InvalidAggregationFunction) {
                throw new InvalidSettingsException(((InvalidAggregationFunction)function).getErrorMessage());
            }
            if (function.hasOptionalSettings()) {
                try {
                    function.configure(tableSpec);
                } catch (InvalidSettingsException e) {
                    throw new InvalidSettingsException("Exception in aggregation function " + function.getLabel()
                        + " of column " + row.getColumnSpec().getName() + ": " + e.getMessage());
                }
            }
            usedColNames.add(row.getColumnSpec().getName());
            m_aggregationFunction2Use.add(row);
        }

        final List<PatternAggregationFunctionRow> patternFunctions = PatternAggregationFunctionRow.loadFunctions(
            m_patternAggregationSettings, functionProvider, tableSpec);

        if (tableSpec.getNumColumns() > usedColNames.size() && !patternFunctions.isEmpty()) {
            for (final DataColumnSpec spec : tableSpec) {
                if (!usedColNames.contains(spec.getName())) {
                    for (final PatternAggregationFunctionRow patternFunction : patternFunctions) {
                        final Pattern pattern = patternFunction.getRegexPattern();
                        final SparkSQLAggregationFunction function = patternFunction.getFunction();
                        if (pattern != null && pattern.matcher(spec.getName()).matches()
                                && function.isCompatible(spec.getType())) {
                            final ColumnAggregationFunctionRow row =
                                    new ColumnAggregationFunctionRow(spec, patternFunction.getFunction());
                            m_aggregationFunction2Use.add(row);
                            usedColNames.add(spec.getName());
                        }
                    }
                }
            }
        }

        final List<DataTypeAggregationFunctionRow> typeFunctions = DataTypeAggregationFunctionRow.loadFunctions(
            m_typeAggregationSettings, functionProvider, tableSpec);
        //check if some columns are left
        if (tableSpec.getNumColumns() > usedColNames.size() && !typeFunctions.isEmpty()) {
            for (final DataColumnSpec spec : tableSpec) {
                if (!usedColNames.contains(spec.getName())) {
                    final DataType dataType = spec.getType();
                    for (final DataTypeAggregationFunctionRow typeAggregator : typeFunctions) {
                        if (typeAggregator.isCompatibleType(dataType)) {
                            final ColumnAggregationFunctionRow row =
                                    new ColumnAggregationFunctionRow(spec, typeAggregator.getFunction());
                            m_aggregationFunction2Use.add(row);
                            usedColNames.add(spec.getName());
                        }
                    }
                }
            }
        }


        return m_aggregationFunction2Use;
    }


    /**
     * Write settings into given configuration object.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to write to.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_manualAggregationSettings.copyTo(settings.addNodeSettings(CFG_MANUAL_AGGREGATION_FUNCTIONS));
        m_patternAggregationSettings.copyTo(settings.addNodeSettings(CFG_PATTERN_AGGREGATION_FUNCTIONS));
        m_typeAggregationSettings.copyTo(settings.addNodeSettings(CFG_TYPE_AGGREGATION_FUNCTIONS));
    }

    /**
     * Reads settings from the given configuration object.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to read from.
     * @throws InvalidSettingsException if loading fails
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_manualAggregationSettings = new NodeSettings(CFG_MANUAL_AGGREGATION_FUNCTIONS);
        settings.getNodeSettings(CFG_MANUAL_AGGREGATION_FUNCTIONS).copyTo(m_manualAggregationSettings);

        m_patternAggregationSettings = new NodeSettings(CFG_PATTERN_AGGREGATION_FUNCTIONS);
        settings.getNodeSettings(CFG_PATTERN_AGGREGATION_FUNCTIONS).copyTo(m_patternAggregationSettings);

        m_typeAggregationSettings = new NodeSettings(CFG_TYPE_AGGREGATION_FUNCTIONS);
        settings.getNodeSettings(CFG_TYPE_AGGREGATION_FUNCTIONS).copyTo(m_typeAggregationSettings);
    }

    /**
     * Validates that the window function settings can be loaded.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to read from.
     * @throws InvalidSettingsException if validation fails
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // only validates the presence, does not actually parse
        settings.getNodeSettings(CFG_MANUAL_AGGREGATION_FUNCTIONS);
        settings.getNodeSettings(CFG_PATTERN_AGGREGATION_FUNCTIONS);
        settings.getNodeSettings(CFG_TYPE_AGGREGATION_FUNCTIONS);
    }


    /**
     * @param functionProvider Provides access to the available aggregation functions.
     * @param spec The spec of the input table
     * @return the list of manual aggregation functions to use
     * @throws InvalidSettingsException
     */
    public List<ColumnAggregationFunctionRow> getManualColumnAggregationFunctionRows(
        final SparkSQLFunctionCombinationProvider functionProvider, final DataTableSpec spec)
        throws InvalidSettingsException {

        return ColumnAggregationFunctionRow.loadFunctions(m_manualAggregationSettings, functionProvider, spec);
    }

    /**
     * @param aggs the list of manual aggregation functions to use
     */
    public void setManualColumnAggregationFunctionRows(final List<ColumnAggregationFunctionRow> aggs) {
        m_manualAggregationSettings = new NodeSettings(CFG_MANUAL_AGGREGATION_FUNCTIONS);
        ColumnAggregationFunctionRow.saveFunctions(m_manualAggregationSettings, aggs);
    }

    /**
     * @param functionProvider Provides access to the available aggregation functions.
     * @param spec The spec of the input table
     * @return the list of type based aggregation functions to use
     * @throws InvalidSettingsException
     */
    public List<DataTypeAggregationFunctionRow> getDataTypeAggregationFunctionRows(
        final SparkSQLFunctionCombinationProvider functionProvider, final DataTableSpec spec)
        throws InvalidSettingsException {

        return DataTypeAggregationFunctionRow.loadFunctions(m_typeAggregationSettings, functionProvider, spec);
    }

    /**
     * @param aggs the list of type based aggregation functions to use
     */
    public void setDataTypeColumnAggregationFunctionRows(final List<DataTypeAggregationFunctionRow> aggs) {
        m_typeAggregationSettings = new NodeSettings(CFG_TYPE_AGGREGATION_FUNCTIONS);
        DataTypeAggregationFunctionRow.saveFunctions(m_typeAggregationSettings, aggs);
    }


    /**
     * @param functionProvider Provides access to the available aggregation functions.
     * @param spec The spec of the input table
     * @return the list of pattern based aggregation functions to use
     * @throws InvalidSettingsException
     */
    public List<PatternAggregationFunctionRow> getPatternAggregationFunctionRows(
        final SparkSQLFunctionCombinationProvider functionProvider, final DataTableSpec spec)
        throws InvalidSettingsException {

        return PatternAggregationFunctionRow.loadFunctions(m_typeAggregationSettings, functionProvider, spec);
    }


    /**
     * @param rows the list of pattern based aggregation functions to use
     */
    public void setPatternAggregationFunctionRows(final List<PatternAggregationFunctionRow> rows) {
        m_patternAggregationSettings = new NodeSettings(CFG_PATTERN_AGGREGATION_FUNCTIONS);
        PatternAggregationFunctionRow.saveFunctions(m_patternAggregationSettings, rows);
    }
}
