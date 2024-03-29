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
 *   Created on 28.06.2015 by koetter
 */
package org.knime.bigdata.spark.node.preproc.renameregex;

import org.knime.base.node.preproc.columnrenameregex.ColumnRenameRegexConfiguration;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.node.preproc.rename.SparkRenameColumnNodeModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkColumnRenameRegexNodeModel extends SparkNodeModel {

    private ColumnRenameRegexConfiguration m_config;

    SparkColumnRenameRegexNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE}, new PortType[] {SparkDataPortObject.TYPE}, false);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec outSpec = createNewSpec(sparkSpec.getTableSpec());
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outSpec)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject inputPort = (SparkDataPortObject) inObjects[0];
        final DataTableSpec outputTableSpec = createNewSpec(inputPort.getTableSpec());
        return new PortObject[] {
            SparkRenameColumnNodeModel.executeRenameColumnJob(inputPort, outputTableSpec, exec)
        };
    }

    private DataTableSpec createNewSpec(final DataTableSpec inSpec) throws InvalidSettingsException {
        if (m_config == null) {
            throw new InvalidSettingsException("No configuration available");
        }
        final DataTableSpec outSpec = m_config.createNewSpec(inSpec);
        if (inSpec.getNumColumns() == 0) {
            // don't bother if input is empty
        } else if (!m_config.hasChanged()) {
            setWarningMessage("Pattern did not match any column "
                    + "name, leaving input unchanged");
        } else if (m_config.hasConflicts()) {
            setWarningMessage("Pattern replace resulted in duplicate column "
                    + "names; resolved conflicts using \"(#index)\" suffix");
        }
        return outSpec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        if (m_config != null) {
            m_config.saveConfiguration(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        ColumnRenameRegexConfiguration config =
            new ColumnRenameRegexConfiguration();
        config.loadSettingsInModel(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        ColumnRenameRegexConfiguration config = new ColumnRenameRegexConfiguration();
        config.loadSettingsInModel(settings);
        m_config = config;
    }
}
