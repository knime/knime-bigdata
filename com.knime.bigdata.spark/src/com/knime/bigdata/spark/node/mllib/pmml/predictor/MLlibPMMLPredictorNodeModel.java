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
package com.knime.bigdata.spark.node.mllib.pmml.predictor;

import java.io.File;

import org.knime.core.data.DataTableSpec;
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
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.hive.utility.HiveUtility;

/**
 *
 * @author koetter
 */
public class MLlibPMMLPredictorNodeModel extends NodeModel {

    private static final String DATABASE_IDENTIFIER = HiveUtility.DATABASE_IDENTIFIER;
    private final SettingsModelString m_tableName = createTableNameModel();
    private final SettingsModelString m_colName = createColumnNameModel();

    /**
     *
     */
    public MLlibPMMLPredictorNodeModel() {
        super(new PortType[]{PMMLPortObject.TYPE, DatabasePortObject.TYPE},
            new PortType[]{DatabasePortObject.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelString createTableNameModel() {
        return new SettingsModelString("tableName", "result");
    }

    /**
     * @return
     */
    static SettingsModelString createColumnNameModel() {
        return new SettingsModelString("columnName", "Cluster");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        DatabasePortObjectSpec spec = (DatabasePortObjectSpec) inSpecs[1];
        if (!spec.getDatabaseIdentifier().equals(DATABASE_IDENTIFIER)) {
            throw new InvalidSettingsException("Only Hive connections are supported");
        }
        return new PortObjectSpec[] {createSQLSpec(spec)};
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
        final PMMLPortObject model = (PMMLPortObject)inObjects[0];
        final DatabasePortObject db = (DatabasePortObject)inObjects[1];
        return new PortObject[] {new DatabasePortObject(createSQLSpec(db.getSpec()))};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_tableName.saveSettingsTo(settings);
        m_colName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_tableName.validateSettings(settings);
        m_colName.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_tableName.loadSettingsFrom(settings);
        m_colName.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) {}

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) {}

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {}
}
