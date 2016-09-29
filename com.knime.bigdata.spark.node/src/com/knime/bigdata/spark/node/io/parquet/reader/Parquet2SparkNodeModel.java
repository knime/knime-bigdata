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
 *   Created on Aug 10, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.parquet.reader;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.JobRunFactory;
import com.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import com.knime.bigdata.spark.core.util.SparkIDs;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Parquet2SparkNodeModel extends SparkSourceNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Parquet2SparkNodeModel.class);

    /** The unique Spark job id.*/
    public static final String JOB_ID = Parquet2SparkNodeModel.class.getCanonicalName();

    private final Parquet2SparkSettings m_settings = new Parquet2SparkSettings();

    /** Constructor. */
    public Parquet2SparkNodeModel() {
        super(new PortType[] {ConnectionInformationPortObject.TYPE}, new PortType[] {SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("HDFS connection information missing");
        }

        final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec) inSpecs[0];
        final ConnectionInformation connInfo = object.getConnectionInformation();

        if (connInfo == null) {
            throw new InvalidSettingsException("No connection information available");
        }

        if (!HDFSRemoteFileHandler.isSupportedConnection(connInfo)) {
            throw new InvalidSettingsException("HDFS connection required");
        }

        m_settings.validateSettings();

        // We cannot provide a spec because it's not clear yet what the file contains
        return new PortObjectSpec[] { null };
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final ConnectionInformationPortObject object = (ConnectionInformationPortObject) inData[0];
        final ConnectionInformation connInfo = object.getConnectionInformation();
        final String inputPath = m_settings.getInputPath();

        final SparkContextID contextID = getContextID(inData);
        ensureContextIsOpen(contextID);
        final JobRunFactory<Parquet2SparkJobInput, Parquet2SparkJobOutput> runFactory = SparkContextUtil.getJobRunFactory(contextID, JOB_ID);

        final String namedOutputObject = SparkIDs.createRDDID();
        LOGGER.info("Loading " + inputPath + " into " + namedOutputObject + " rdd");
        final Parquet2SparkJobInput jobInput = new Parquet2SparkJobInput(namedOutputObject, inputPath);
        final Parquet2SparkJobOutput jobOutput = runFactory.createRun(jobInput).run(contextID, exec);

        final DataTableSpec outputSpec = KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(namedOutputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, namedOutputObject, outputSpec);
        final SparkDataPortObject sparkObject = new SparkDataPortObject(resultTable);

        return new PortObject[] { sparkObject };
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedSettingsFrom(settings);
    }
}