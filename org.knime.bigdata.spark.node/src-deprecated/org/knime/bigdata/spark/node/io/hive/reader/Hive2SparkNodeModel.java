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
package org.knime.bigdata.spark.node.io.hive.reader;

import org.knime.bigdata.hive.utility.HiveUtility;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
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

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@Deprecated
public class Hive2SparkNodeModel extends SparkSourceNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Hive2SparkNodeModel.class);

    /**The unique Spark job id.*/
    public static final String JOB_ID = Hive2SparkNodeModel.class.getCanonicalName();

    private final boolean m_isDeprecatedNode;

    /**
     * Constructor.
     *
     * @param isDeprecatedNode Whether this node model instance should emulate the behavior of the deprecated
     *            table2spark node model.
     */
    public Hive2SparkNodeModel(final boolean isDeprecatedNode) {
        super(new PortType[] {DatabasePortObject.TYPE}, isDeprecatedNode, new PortType[] {SparkDataPortObject.TYPE});
        m_isDeprecatedNode = isDeprecatedNode;
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
                new SparkDataPortObjectSpec(getContextID(inSpecs), spec.getDataTableSpec());
        return new PortObjectSpec[] {resultSpec};
    }


    /**
     * Checks whether the input Database is compatible.
     * @param spec the {@link DatabasePortObjectSpec} from the input port
     * @throws InvalidSettingsException If the wrong database is connected
     */
    protected void checkDatabaseIdentifier(final DatabasePortObjectSpec spec) throws InvalidSettingsException {
        if (!spec.getDatabaseIdentifier().contains(HiveUtility.DATABASE_IDENTIFIER)) {
            throw new InvalidSettingsException("Input must be a Hive connection");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final SparkContextID contextID = getContextID(inData);

        if (m_isDeprecatedNode) {
            exec.setMessage("Creating a Spark context...");
            ensureContextIsOpen(contextID, exec.createSubProgress(0.1));
        }

        final DatabasePortObject db = (DatabasePortObject)inData[0];
        final DatabaseQueryConnectionSettings settings = db.getConnectionSettings(getCredentialsProvider());
        final DataTableSpec resultTableSpec = db.getSpec().getDataTableSpec();
        final String hiveQuery = settings.getQuery();
        final SparkDataTable resultTable = new SparkDataTable(contextID, resultTableSpec);
        LOGGER.debug("Original sql: " + hiveQuery);
        final Hive2SparkJobInput jobInput = new Hive2SparkJobInput(resultTable.getID(), hiveQuery);
        LOGGER.debug("Cleaned sql: " + jobInput.getQuery());
        SparkContextUtil.getJobRunFactory(contextID, JOB_ID).createRun(jobInput).run(contextID, exec);
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