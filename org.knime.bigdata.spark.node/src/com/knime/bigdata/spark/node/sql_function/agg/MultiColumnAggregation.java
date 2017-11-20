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
 *   Created on Nov 16, 2017 by Sascha Wolke, KNIME GmbH
 */
package com.knime.bigdata.spark.node.sql_function.agg;

import java.awt.Component;
import java.io.Serializable;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.util.ColumnFilter;

import com.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import com.knime.bigdata.spark.node.sql_function.SparkSQLAggregationFunction;
import com.knime.bigdata.spark.node.sql_function.SparkSQLFunctionDialogFactory;

/**
 * Aggregate function with multiple input columns.
 * @author Sascha Wolke, KNIME GmbH
 */
public class MultiColumnAggregation implements SparkSQLAggregationFunction {
    private final String m_id;
    private final String m_description;

    private final SettingsModelFilterString m_otherColumnsSettings;
    private final DialogComponentColumnFilter m_otherColumnsFilterComponent;

    private DataColumnSpec m_colSpec;

    /** Function factory */
    public static class Factory implements SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> {
        private final String m_id;
        private final String m_description;

        /**
         * Aggregation function that is compatible to every data type and returns input data type as result.
         * @param id the name of the function
         * @param description the description
         */
        public Factory(final String id, final String description) {
            m_id = id;
            m_description = description;
        }

        @Override
        public String getId() { return m_id; }

        @Override
        public SparkSQLAggregationFunction getInstance() {
            return new MultiColumnAggregation(m_id, m_description);
        }
    }

    /**
     * @param name the name of the function
     * @param description the description
     */
    public MultiColumnAggregation(final String name, final String description) {
        m_id = name;
        m_description = description;

        m_otherColumnsSettings = new SettingsModelFilterString("otherColumns");
        m_otherColumnsFilterComponent =
            new DialogComponentColumnFilter(m_otherColumnsSettings, 0, true, new ColumnFilter() {
                @Override
                public boolean includeColumn(final DataColumnSpec colSpec) {
                    return m_colSpec == null || !m_colSpec.equalStructure(colSpec);
                }

                @Override
                public String allFilteredMsg() {
                    return "No applicable column available";
                }
            }, false);
    }

    @Override
    public void setColumnSpec(final DataColumnSpec colSpec) {
        m_colSpec = colSpec;
    }

    @Override
    public DataType getType(final DataType originalType) {
        return originalType;
    }

    @Override
    public boolean isCompatible(final DataType type) {
        return true;
    }

    @Override
    public String getId() {
        return m_id;
    }

    @Override
    public String getLabel() {
        return m_id;
    }

    /** @return all column names */
    private String[] getColumns(final String columnName, final DataTableSpec inSpec) {
        final HashSet<Serializable> columns = new HashSet<>();
        columns.add(columnName);

        for (String col : m_otherColumnsSettings.getIncludeList()) {
            if (!columnName.equals(col)) {
                columns.add(col);
            }
        }

        return columns.toArray(new String[0]);
    }

    @Override
    public String getFuncNameColNameLabel(final String columnName, final DataTableSpec inSpec) {
        return getId() + "(" + StringUtils.join(getColumns(columnName, inSpec), ",") + ")";
    }

    @Override
    public String getColNameFuncNameLabel(final String columnName, final DataTableSpec inSpec) {
        return StringUtils.join(getColumns(columnName, inSpec), "_") + "(" + getId() + ")";
    }

    @Override
    public String getDescription() {
        return m_description;
    }

    @Override
    public boolean hasOptionalSettings() {
        return true;
    }

    @Override
    public Component getSettingsPanel() {
        return m_otherColumnsFilterComponent.getComponentPanel();
    }

    @Override
    public SparkSQLFunctionJobInput getSparkJobInput(final String factory,
        final String inColName, final String outColName, final DataTableSpec inSpec) {

        final String args[] = getColumns(inColName, inSpec);
        final IntermediateDataType argTypes[] = new IntermediateDataType[args.length];

        for (int i = 0; i < args.length; i++) {
            argTypes[i] = IntermediateDataTypes.STRING;
        }

        return new SparkSQLFunctionJobInput(m_id, factory, outColName, args, argTypes);
    }

    @Override
    public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_otherColumnsSettings.loadSettingsFrom(settings);
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec spec) throws NotConfigurableException {
        m_otherColumnsFilterComponent.loadSettingsFrom(settings, new DataTableSpec[] { spec });
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_otherColumnsSettings.saveSettingsTo(settings);
    }

    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_otherColumnsSettings.loadSettingsFrom(settings);
    }

    @Override
    public void validate() throws InvalidSettingsException {
        // nothing to do
    }

    @Override
    public void configure(final DataTableSpec spec) throws InvalidSettingsException {
        // nothing to do
    }
}
