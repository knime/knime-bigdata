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
package org.knime.bigdata.spark.node.sql_function.agg;

import java.awt.Component;
import java.io.Serializable;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;

import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import org.knime.bigdata.spark.node.sql_function.SparkSQLAggregationFunction;
import org.knime.bigdata.spark.node.sql_function.SparkSQLFunctionDialogFactory;

/**
 * Returns the last value in a group aggregation function.
 * @author Sascha Wolke, KNIME GmbH
 */
public class LastAggregation implements SparkSQLAggregationFunction {
    private final static String ID = "last";
    private final static String DESC = "Returns the last value in a group";

    private final SettingsModelBoolean m_ignoreNullsModel = new SettingsModelBoolean("ignoreNulls", false);
    private final DialogComponentBoolean m_ignoreNullsComponent =
            new DialogComponentBoolean(m_ignoreNullsModel, "ignore nulls");

    /** Function factory */
    public static class Factory implements SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> {
        @Override
        public String getId() { return ID; }

        @Override
        public SparkSQLAggregationFunction getInstance() { return new LastAggregation(); }
    }

    /** Default constructor. */
    public LastAggregation() {
    }

    @Override
    public void setColumnSpec(final DataColumnSpec colSpec) {
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
        return ID;
    }

    @Override
    public String getLabel() {
        return ID;
    }

    @Override
    public String getFuncNameColNameLabel(final String columnName, final DataTableSpec inSpec) {
        return getId() + "(" + columnName + ")";
    }

    @Override
    public String getColNameFuncNameLabel(final String columnName, final DataTableSpec inSpec) {
        return columnName + "(" + getId() + ")";
    }

    @Override
    public String getDescription() {
        return DESC;
    }

    @Override
    public boolean hasOptionalSettings() {
        return true;
    }

    @Override
    public Component getSettingsPanel() {
        return m_ignoreNullsComponent.getComponentPanel();
    }

    @Override
    public SparkSQLFunctionJobInput getSparkJobInput(final String factory,
        final String inColName, final String outColName, final DataTableSpec inSpec) {

        return new SparkSQLFunctionJobInput(ID, factory, outColName,
            new Serializable[] { inColName, m_ignoreNullsModel.getBooleanValue() },
            new IntermediateDataType[] { IntermediateDataTypes.STRING, IntermediateDataTypes.BOOLEAN });
    }

    @Override
    public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_ignoreNullsModel.loadSettingsFrom(settings);
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec spec) throws NotConfigurableException {
        m_ignoreNullsComponent.loadSettingsFrom(settings, new DataTableSpec[] { spec });
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_ignoreNullsModel.saveSettingsTo(settings);

    }

    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_ignoreNullsModel.validateSettings(settings);

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
