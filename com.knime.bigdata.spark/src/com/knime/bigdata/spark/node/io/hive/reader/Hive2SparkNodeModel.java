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
package com.knime.bigdata.spark.node.io.hive.reader;

import java.net.ConnectException;

import javax.ws.rs.ProcessingException;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;

import com.knime.bigdata.hive.utility.HiveUtility;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;

/**
 *
 * @author koetter
 */
public class Hive2SparkNodeModel extends SparkNodeModel {

    /**
     * Constructor.
     */
    public Hive2SparkNodeModel() {
        super(new PortType[] {DatabasePortObject.TYPE}, new PortType[] {SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        HiveUtility.LICENSE_CHECKER.checkLicenseInNode();
        if (inSpecs == null || inSpecs.length != 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Hive query found");
        }
        final DatabasePortObjectSpec spec = (DatabasePortObjectSpec)inSpecs[0];
        if (!HiveUtility.DATABASE_IDENTIFIER.equals(spec.getDatabaseIdentifier())) {
            throw new InvalidSettingsException("Input must be a Hive connection");
        }
        final SparkDataPortObjectSpec resultSpec = new SparkDataPortObjectSpec(getContext(), spec.getDataTableSpec());
        return new PortObjectSpec[] {resultSpec};
    }

    private KNIMESparkContext getContext() throws InvalidSettingsException {
        try {
            return KnimeContext.getSparkContext();
        } catch (Exception e) {
            if (e instanceof ProcessingException) {
                final Throwable cause = e.getCause();
                if (cause != null && (cause instanceof ConnectException)) {
                    throw new InvalidSettingsException("Unable to connect to Spark job server. Exception: "
                            + cause.getMessage());
                }
            }
            if (e instanceof ConnectException) {
                throw new InvalidSettingsException("Unable to connect to Spark job server. Exception: "
                        + e.getMessage());
            }
            throw new InvalidSettingsException(e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final DatabasePortObject db = (DatabasePortObject)inData[0];
        final DatabaseQueryConnectionSettings settings = db.getConnectionSettings(getCredentialsProvider());
        final DataTableSpec resultTableSpec = db.getSpec().getDataTableSpec();
        final String hiveQuery = settings.getQuery();
        final SparkDataTable resultTable = new SparkDataTable(getContext(), resultTableSpec);
        final HiveToRDDTask hiveToRDDTask = new HiveToRDDTask(resultTable, hiveQuery);
        hiveToRDDTask.execute(exec);
        final SparkDataPortObject sparkObject = new SparkDataPortObject(resultTable);
        return new PortObject[] {sparkObject};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        //nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        //nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        //nothing to do
    }
}
