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
 *   Created on 27.05.2015 by koetter
 */
package com.knime.bigdata.spark.node.io.hive.reader;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;

import com.knime.bigdata.hive.utility.HiveUtility;
import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import com.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Hive2SparkNodeModel extends SparkSourceNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Hive2SparkNodeModel.class);

    /**The unique Spark job id.*/
    public static final String JOB_ID = Hive2SparkNodeModel.class.getCanonicalName();

    /**
     * Constructor.
     * @param optionalSparkPort true if input spark context port is optional
     */
    public Hive2SparkNodeModel(final boolean optionalSparkPort) {
        super(new PortType[] {DatabasePortObject.TYPE}, optionalSparkPort, new PortType[] {SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Hive query found");
        }
        final DatabasePortObjectSpec spec = (DatabasePortObjectSpec)inSpecs[0];

        checkDatabaseIdentifier(spec);

        final SparkDataPortObjectSpec resultSpec =
                new SparkDataPortObjectSpec(getContextID(inSpecs), spec.getDataTableSpec(), getKNIMESparkExecutorVersion());
        return new PortObjectSpec[] {resultSpec};
    }


    /**
     * Checks whether the input Database is compatible.
     * @param spec the {@link DatabasePortObjectSpec} from the input port
     * @throws InvalidSettingsException If the wrong database is connected
     */
    protected void checkDatabaseIdentifier(final DatabasePortObjectSpec spec) throws InvalidSettingsException {
        if (!HiveUtility.DATABASE_IDENTIFIER.equals(spec.getDatabaseIdentifier())) {
            throw new InvalidSettingsException("Input must be an Hive connection");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final SparkContextID contextID = getContextID(inData);
        ensureContextIsOpen(contextID);
        final SimpleJobRunFactory<JobInput> runFactory = SparkContextUtil.getSimpleRunFactory(contextID, JOB_ID);
        final DatabasePortObject db = (DatabasePortObject)inData[0];
        final DatabaseQueryConnectionSettings settings = db.getConnectionSettings(getCredentialsProvider());
        final DataTableSpec resultTableSpec = db.getSpec().getDataTableSpec();
        final String hiveQuery = settings.getQuery();
        final SparkDataTable resultTable = new SparkDataTable(contextID, resultTableSpec, getKNIMESparkExecutorVersion());
        LOGGER.debug("Original sql: " + hiveQuery);
        final Hive2SparkJobInput jobInput = new Hive2SparkJobInput(resultTable.getID(), hiveQuery);
        LOGGER.debug("Cleaned sql: " + jobInput.getQuery());
        runFactory.createRun(jobInput).run(contextID, exec);
        final SparkDataPortObject sparkObject = new SparkDataPortObject(resultTable);
        return new PortObject[] {sparkObject};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        //nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        //nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        //nothing to do
    }
}