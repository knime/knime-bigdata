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
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.sampling;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Locale;

import org.knime.base.node.preproc.sample.SamplingNodeSettings;
import org.knime.base.node.preproc.sample.SamplingNodeSettings.SamplingMethods;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;

import com.knime.bigdata.hive.utility.HiveUtility;

/**
 *
 * @author koetter
 */
public class MLlibSamplingNodeModel extends NodeModel {

    private static final String DATABASE_IDENTIFIER = HiveUtility.DATABASE_IDENTIFIER;
    private final SamplingNodeSettings m_settings = new SamplingNodeSettings();
    private final SettingsModelString m_tableName = createTableNameModel();

    /**
     *
     */
    public MLlibSamplingNodeModel() {
        super(new PortType[]{DatabasePortObject.TYPE},
            new PortType[]{DatabasePortObject.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelString createTableNameModel() {
        return new SettingsModelString("tableName", "result");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        DatabasePortObjectSpec spec = (DatabasePortObjectSpec) inSpecs[0];
        if (!spec.getDatabaseIdentifier().equals(DATABASE_IDENTIFIER)) {
            throw new InvalidSettingsException("Only Hive connections are supported");
        }
        checkSettings(spec.getDataTableSpec());
        return new PortObjectSpec[] {createSQLSpec(spec)};
    }

    /**
     * Checks if the node settings are valid, i.e. a method has been set and the
     * class column exists if stratified sampling has been chosen.
     *
     * @param inSpec the input table's spec
     * @throws InvalidSettingsException if the settings are invalid
     */
    protected void checkSettings(final DataTableSpec inSpec)
            throws InvalidSettingsException {
        if (m_settings.countMethod() == null) {
            throw new InvalidSettingsException("No sampling method selected");
        }
        if (m_settings.samplingMethod().equals(SamplingMethods.Stratified)
                && !inSpec.containsName(m_settings.classColumn())) {
            throw new InvalidSettingsException("Column '"
                    + m_settings.classColumn() + "' for stratified sampling "
                    + "does not exist");
        }
    }

    private DatabasePortObjectSpec createSQLSpec(final DatabasePortObjectSpec spec) throws InvalidSettingsException {
        final DataTableSpec tableSpec = spec.getDataTableSpec();
        final DatabaseQueryConnectionSettings conn = spec.getConnectionSettings(getCredentialsProvider());
        final DatabaseUtility utility = DatabaseUtility.getUtility(DATABASE_IDENTIFIER);
        final StatementManipulator sm = utility.getStatementManipulator();
        conn.setQuery("select * from " + sm.quoteIdentifier(m_tableName.getStringValue()));
        return new DatabasePortObjectSpec(tableSpec, conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final DatabasePortObject db = (DatabasePortObject)inObjects[0];
        return new PortObject[] {db};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
        m_tableName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        SamplingNodeSettings temp = new SamplingNodeSettings();
        temp.loadSettingsFrom(settings, false);

        if (temp.countMethod() == SamplingNodeSettings.CountMethods.Relative) {
            if (temp.fraction() < 0.0 || temp.fraction() > 1.0) {
                NumberFormat f = NumberFormat.getPercentInstance(Locale.US);
                String p = f.format(100.0 * temp.fraction());
                throw new InvalidSettingsException("Invalid percentage: " + p);
            }
        } else if (temp.countMethod() == SamplingNodeSettings.CountMethods.Absolute) {
            if (temp.count() < 0) {
                throw new InvalidSettingsException("Invalid count: "
                        + temp.count());
            }
        } else {
            throw new InvalidSettingsException("Unknown method: "
                    + temp.countMethod());
        }

        if (temp.samplingMethod().equals(SamplingMethods.Stratified)
                && (temp.classColumn() == null)) {
            throw new InvalidSettingsException(
                    "No class column for stratified sampling selected");
        }
        final String tableName = ((SettingsModelString)m_tableName.createCloneWithValidatedValue(settings)).getStringValue();
        if (tableName == null || tableName.isEmpty()) {
            throw new InvalidSettingsException("Please specify the table name");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings, false);
        m_tableName.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
    CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
    CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // nothing to do
    }
}
