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
package org.knime.bigdata.spark.node.sql_function;

import java.awt.Component;
import java.io.Serializable;

import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.database.aggregation.AggregationFunction;

/**
 * Spark SQL function without additional settings.
 * @author Sascha Wolke, KNIME GmbH
 */
public abstract class NoSettingsFunction implements SparkSQLFunction, AggregationFunction {
    private final String m_id;
    private final String m_description;

    /**
     * @param id unique identifier of this functions
     * @param description a short description of this function
     */
    public NoSettingsFunction(final String id, final String description) {
        m_id = id;
        m_description = description;
    }

    @Override
    public String getId() {
        return m_id;
    }

    @Override
    public String getLabel() {
        return m_id;
    }

    @Override
    public String getFuncNameColNameLabel(final String columnName, final DataTableSpec inSpec) {
        return getId() + "(" + columnName + ")";
    }

    @Override
    public String getColNameFuncNameLabel(final String columnName, final DataTableSpec inSpec) {
        return columnName + "(" + getId() + ")";
    }

    /** @return the name of the function used in the column name */
    public String getColumnName() {
        return getLabel();
    }

    /**
     * Set column spec to aggregate on.
     * @param colSpec column to aggregate on
     */
    public void setColumnSpec(final DataColumnSpec colSpec) {}

    @Override
    public boolean isCompatible(final DataType type) {
        return true;
    }

    @Override
    public String getDescription() {
        return m_description;
    }

    @Override
    public boolean hasOptionalSettings() {
        return false;
    }

    @Override
    public Component getSettingsPanel() {
        return null;
    }

    @Override
    public SparkSQLFunctionJobInput getSparkJobInput(final String factory,
        final String inColName, final String outColName, final DataTableSpec inSpec) {

        return new SparkSQLFunctionJobInput(m_id, factory, outColName,
            new Serializable[] { inColName },
            new IntermediateDataType[] { IntermediateDataTypes.STRING });
    }

    @Override
    public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {}

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec spec) throws NotConfigurableException {}

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {}

    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {}

    @Override
    public void validate() throws InvalidSettingsException {}

    @Override
    public void configure(final DataTableSpec spec) throws InvalidSettingsException {}
}
