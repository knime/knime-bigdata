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

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import org.knime.bigdata.spark.node.sql_function.SparkSQLAggregationFunction;
import org.knime.bigdata.spark.node.sql_function.SparkSQLFunctionDialogFactory;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Aggregate function with two input columns.
 * @author Sascha Wolke, KNIME GmbH
 */
public class TwoColumnAggregation implements SparkSQLAggregationFunction {
    private final String m_id;
    private final String m_label;
    private final String m_description;
    private final Class<DataValue> m_compatibleClass;

    private final SettingsModelString m_otherColumnModel;
    private final DialogComponentColumnNameSelection m_otherColumnComponent;

    /** Function factory */
    public static class Factory implements SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> {
        private final String m_id;
        private final String m_description;
        private final Class<DataValue> m_compatibleClass;

        /**
         * @param id the id of the function
         * @param description the description
         * @param compatibleClass the compatible {@link DataValue} class or <code>null</code>
         */
        @SuppressWarnings("unchecked")
        public Factory(final String id, final String description, final Class<? extends DataValue> compatibleClass) {
            m_id = id;
            m_description = description;
            m_compatibleClass = (Class<DataValue>)compatibleClass;
        }

        /**
         * Aggregation function that is compatible to every data type and returns input data type as result.
         * @param id the name of the function
         * @param description the description
         */
        public Factory(final String id, final String description) {
            this(id, description, null);
        }

        @Override
        public String getId() { return m_id; }

        @Override
        public SparkSQLAggregationFunction getInstance() {
            return new TwoColumnAggregation(m_id, m_description, m_compatibleClass);
        }
    }

    /**
     * @param name the name of the function
     * @param description the description
     * @param compatibleClass the compatible {@link DataValue} class or <code>null</code>
     */
    @SuppressWarnings("unchecked")
    public TwoColumnAggregation(final String name, final String description, final Class<? extends DataValue> compatibleClass) {
        m_id = name;
        m_label = name;
        m_description = description;
        m_compatibleClass = (Class<DataValue>)compatibleClass;

        m_otherColumnModel = new SettingsModelString("otherColumn", "");
        m_otherColumnComponent = new DialogComponentColumnNameSelection(
            m_otherColumnModel, "other column", 0, true, m_compatibleClass);
    }

    /**
     * Aggregation function that is compatible to every data type and returns input data type as result.
     * @param name the name of the function
     * @param description the description
     */
    public TwoColumnAggregation(final String name, final String description) {
        this(name, description, null);
    }

    @Override
    public void setColumnSpec(final DataColumnSpec colSpec) {
    }

    @Override
    public boolean isCompatible(final DataType type) {
        return m_compatibleClass == null || type.isCompatible(m_compatibleClass);
    }

    @Override
    public String getId() {
        return m_id;
    }

    @Override
    public String getLabel() {
        return m_label;
    }

    @Override
    public String getFuncNameColNameLabel(final String columnName, final DataTableSpec inSpec) {
        return getId() + "(" + columnName + "," + m_otherColumnModel.getStringValue() + ")";
    }

    @Override
    public String getColNameFuncNameLabel(final String columnName, final DataTableSpec inSpec) {
        return columnName + "_" + m_otherColumnModel.getStringValue() + "(" + getId() + ")";
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
        return m_otherColumnComponent.getComponentPanel();
    }

    @Override
    public SparkSQLFunctionJobInput getSparkJobInput(final String factory,
        final String inColName, final String outColName, final DataTableSpec inSpec) {

        return new SparkSQLFunctionJobInput(m_id, factory, outColName,
            new Serializable[] { inColName, m_otherColumnModel.getStringValue() },
            new IntermediateDataType[] { IntermediateDataTypes.STRING, IntermediateDataTypes.STRING });
    }

    @Override
    public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_otherColumnModel.loadSettingsFrom(settings);
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec spec) throws NotConfigurableException {
        m_otherColumnComponent.loadSettingsFrom(settings, new DataTableSpec[] { spec });
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_otherColumnModel.saveSettingsTo(settings);
    }

    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_otherColumnModel.validateSettings(settings);
    }

    @Override
    public void validate() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_otherColumnModel.getStringValue())) {
            throw new InvalidSettingsException("Second column name  in " + getId() + " aggregation required.");
        }
    }

    @Override
    public void configure(final DataTableSpec spec) throws InvalidSettingsException {
        // nothing to do
    }
}
