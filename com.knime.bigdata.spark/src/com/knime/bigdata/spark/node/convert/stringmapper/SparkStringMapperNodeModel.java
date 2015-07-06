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
 *   Created on 06.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.convert.stringmapper;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.StringValue;
import org.knime.core.data.container.CellFactory;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkStringMapperNodeModel extends AbstractSparkNodeModel {

    private static final DataType MAP_TYPE = DoubleCell.TYPE;

    private final SettingsModelString m_col = createColModel();

    private final SettingsModelString m_columnName = createColNameModel();

    SparkStringMapperNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE},
            new PortType[] {SparkDataPortObject.TYPE, SparkDataPortObject.TYPE});
    }

    /**
     * @return the column name to convert
     */
    static SettingsModelString createColModel() {
        return new SettingsModelString("columnName", null);
    }

    /**
     * @return the name of the mapping column
     */
    static SettingsModelString createColNameModel() {
        return new SettingsModelString("newColumnName", "Class");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input spec available");
        }
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec spec = sparkSpec.getTableSpec();
        final String colName = m_col.getStringValue();
        DataColumnSpec origColSpec = null;
        if (colName == null) {
            //preset it with the first nominal column
            for (DataColumnSpec colSpec : spec) {
                if (colSpec.getType().isCompatible(StringValue.class)) {
                    m_col.setStringValue(colSpec.getName());
                    setWarningMessage("Column name preset to " + colSpec.getName());
                    origColSpec = colSpec;
                    break;
                }
            }
        } else {
            origColSpec = spec.getColumnSpec(colName);
        }
        if (origColSpec == null) {
            throw new InvalidSettingsException("Column " + colName + " not found in input spec");
        }
        final DataTableSpec firstSpec = createAppendSpec(spec);
        final DataTableSpec secondSpec = createMapSpec(spec, origColSpec);
        return new PortObjectSpec[] {new SparkDataPortObjectSpec(sparkSpec.getContext(), firstSpec),
            new SparkDataPortObjectSpec(sparkSpec.getContext(), secondSpec)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[0];
        final DataTableSpec spec = rdd.getTableSpec();
        final int colIdx = spec.findColumnIndex(m_col.getStringValue());
        if (colIdx < 0) {
            throw new InvalidSettingsException("Column " + m_col.getStringValue() + " not found");
        }
        final DataTableSpec firstSpec = createAppendSpec(spec);
        final DataTableSpec secondSpec = createMapSpec(spec, spec.getColumnSpec(colIdx));
        return null;
    }

    private DataTableSpec createMapSpec(final DataTableSpec spec, final DataColumnSpec origColSpec) {
        final DataColumnSpec[] specs = new DataColumnSpec[2];
        final DataColumnSpecCreator creator = new DataColumnSpecCreator(origColSpec);
        specs[0] = creator.createSpec();
        specs[1] = createMapColSpec(spec);
        return new DataTableSpec(specs);
    }

    /**
     * @param spec
     * @param creator
     * @return
     */
    private DataColumnSpec createMapColSpec(final DataTableSpec spec) {
        return new DataColumnSpecCreator(DataTableSpec.getUniqueColumnName(spec, m_columnName.getStringValue()),
            MAP_TYPE).createSpec();
    }

    private DataTableSpec createAppendSpec(final DataTableSpec spec) {
        final ColumnRearranger rearranger = new ColumnRearranger(spec);
        rearranger.append(new CellFactory() {

            @Override
            public void setProgress(final int curRowNr, final int rowCount, final RowKey lastKey, final ExecutionMonitor exec) {
                //nothing to do
            }

            @Override
            public DataColumnSpec[] getColumnSpecs() {
                return new DataColumnSpec[] {createMapColSpec(spec)};
            }

            @Override
            public DataCell[] getCells(final DataRow row) {
                return null;
            }
        });
        return rearranger.createSpec();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_col.saveSettingsTo(settings);
        m_columnName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        String colName = ((SettingsModelString)m_col.createCloneWithValidatedValue(settings)).getStringValue();
        if (colName == null || colName.isEmpty()) {
            throw new InvalidSettingsException("Column name must not be empty");
        }
        colName = ((SettingsModelString)m_columnName.createCloneWithValidatedValue(settings)).getStringValue();
        if (colName == null || colName.isEmpty()) {
            throw new InvalidSettingsException("New column name must not be empty");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_col.loadSettingsFrom(settings);
        m_columnName.loadSettingsFrom(settings);
    }
}
