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
 *   Created on 27.05.2015 by koetter
 */
package com.knime.bigdata.spark.node.io.hive.writer;

import java.sql.Connection;

import org.apache.spark.sql.types.StructType;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;

import com.knime.bigdata.hive.utility.HiveUtility;
import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.util.SparkUtil;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark2HiveNodeModel extends SparkNodeModel {

    private final SettingsModelString m_tableName = createTableNameModel();

    private final SettingsModelBoolean m_dropExisting = createDropExistingModel();

    /**
     * Constructor.
     */
    Spark2HiveNodeModel() {
        super(new PortType[] {DatabaseConnectionPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[] {DatabasePortObject.TYPE});
    }

    /**
     * @return the drop existing table model
     */
    static SettingsModelBoolean createDropExistingModel() {
        return new SettingsModelBoolean("dropExistingTable", false);
    }

    /**
     * @return the table name model
     */
    static SettingsModelString createTableNameModel() {
        return new SettingsModelString("tableName", "sparkTable");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        HiveUtility.LICENSE_CHECKER.checkLicenseInNode();
        if (inSpecs == null || inSpecs.length != 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("No input data found");
        }

        final DatabaseConnectionPortObjectSpec spec = (DatabaseConnectionPortObjectSpec)inSpecs[0];
        if (!HiveUtility.DATABASE_IDENTIFIER.equals(spec.getDatabaseIdentifier())) {
            throw new InvalidSettingsException("Input must be a Hive connection");
        }
        final DataTableSpec tableSpec = ((SparkDataPortObjectSpec)inSpecs[1]).getTableSpec();
        final DatabasePortObjectSpec resultSpec = createResultSpec(spec, tableSpec);
        return new PortObjectSpec[] {resultSpec};
    }

    private DatabasePortObjectSpec createResultSpec(final DatabaseConnectionPortObjectSpec spec, final DataTableSpec tableSpec)
        throws InvalidSettingsException {
        //TK_TODO: take into account that hive does not support upper case columns
        final String query = "SELECT * FROM " + m_tableName.getStringValue();
        DatabasePortObjectSpec resultSpec = new DatabasePortObjectSpec(tableSpec,
            new DatabaseQueryConnectionSettings(spec.getConnectionSettings(getCredentialsProvider()), query));
        return resultSpec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final DatabaseConnectionPortObject con = (DatabaseConnectionPortObject)inData[0];
        final DatabaseConnectionSettings settings = con.getConnectionSettings(getCredentialsProvider());
        final DatabaseUtility utility = settings.getUtility();
        final String tableName = m_tableName.getStringValue();
        try (final Connection connection = settings.createConnection(getCredentialsProvider());) {
            if (utility.tableExists(connection, tableName)) {
                if (m_dropExisting.getBooleanValue()) {
                    final String dropTableStmt = utility.getStatementManipulator().dropTable(tableName, false);
                    settings.execute(dropTableStmt, getCredentialsProvider());
                } else {
                    throw new InvalidSettingsException("Table " + tableName + " already exists");
                }
            }
        }
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[1];
        final DataTableSpec spec = rdd.getTableSpec();
        final StructType schema = SparkUtil.toStructType(spec);
        final RDDToHiveTask task = new RDDToHiveTask(rdd.getData(), tableName, schema);
        task.execute(exec);
        final DatabasePortObjectSpec resultSpec = createResultSpec(con.getSpec(), rdd.getTableSpec());
        return new PortObject[] {new DatabasePortObject(resultSpec)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_tableName.saveSettingsTo(settings);
        m_dropExisting.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final String name = ((SettingsModelString)m_tableName.createCloneWithValidatedValue(settings)).getStringValue();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Table name must not be empty");
        }
        m_dropExisting.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_tableName.loadSettingsFrom(settings);
        m_dropExisting.loadSettingsFrom(settings);
    }
}