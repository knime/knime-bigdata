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
 *   Created on Aug 10, 2016 by sascha
 */
package org.knime.bigdata.spark.node.io.genericdatasource.reader;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.core.runtime.URIUtil;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.base.filehandling.remote.files.RemoteFileHandlerRegistry;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.jar.bundle.BundleGroupSparkJarRegistry;
import org.knime.bigdata.spark.core.job.JobWithFilesRunFactory;
import org.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * @author Sascha Wolke, KNIME.com
 * @param <T> Settings type.
 */
public class GenericDataSource2SparkNodeModel<T extends GenericDataSource2SparkSettings> extends SparkSourceNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(GenericDataSource2SparkNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = GenericDataSource2SparkNodeModel.class.getCanonicalName();

    /** Internal settings object. */
    private final T m_settings;

    private final boolean m_isDeprecatedNode;

    /**
     * Default constructor.
     * @param settings - Initial settings
     * @param isDeprecatedNode Whether this node model instance should emulate the behavior of the deprecated
     *            table2spark node model.
     */
    public GenericDataSource2SparkNodeModel(final T settings, final boolean isDeprecatedNode) {
        super(new PortType[] {ConnectionInformationPortObject.TYPE}, isDeprecatedNode, new PortType[] {SparkDataPortObject.TYPE});
        m_settings = settings;
        m_isDeprecatedNode = isDeprecatedNode;
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

        if (!HDFSRemoteFileHandler.isSupportedConnection(connInfo)
                && !(connInfo instanceof CloudConnectionInformation)
                && !(RemoteFileHandlerRegistry.getProtocol(connInfo.getProtocol()).getName().contains("hdfs"))
                && !(RemoteFileHandlerRegistry.getProtocol(connInfo.getProtocol()).getName().equalsIgnoreCase("dbfs"))) {
            throw new InvalidSettingsException("HDFS, DBFS or cloud connection required");
        }

        m_settings.validateSettings();

        final SparkVersion version = SparkContextUtil.getSparkVersion(getContextID(inSpecs));
        if (!m_settings.isCompatibleSparkVersion(version)) {
            throw new InvalidSettingsException("Unsupported Spark Version! This node requires at least Spark " + m_settings.getMinSparkVersion() + ".");
        }

        // We cannot provide a spec because it's not clear yet what the file contains
        return new PortObjectSpec[] { null };
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final ConnectionInformationPortObject object = (ConnectionInformationPortObject) inData[0];
        final ConnectionInformation connInfo = object.getConnectionInformation();
        final SparkContextID contextID = getContextID(inData);
        final SparkDataTable resultTable = runJob(m_settings, m_isDeprecatedNode, contextID, connInfo, exec);
        final SparkDataPortObject sparkObject = new SparkDataPortObject(resultTable);

        return new PortObject[] { sparkObject };
    }

    /**
     * Run job with given settings for preview table.
     * @param settings - Settings to use.
     * @param contextID - Context to run on.
     * @param connectionInfo - Connection info to use.
     * @param exec - Execution monitor to use.
     * @return Result table
     * @throws Exception
     */
    public static <T extends GenericDataSource2SparkSettings> SparkDataTable preparePreview(final T settings, final SparkContextID contextID,
            final ConnectionInformation connectionInfo, final ExecutionMonitor exec) throws Exception {

        exec.setMessage("Running spark job and fetching preview");
        return runJob(settings, false, contextID, connectionInfo, exec);
    }

    private static <T extends GenericDataSource2SparkSettings> SparkDataTable runJob(final T settings,
        final boolean isDeprecatedNode, final SparkContextID contextID, final ConnectionInformation connectionInfo,
        final ExecutionMonitor exec) throws Exception {

        final String format = settings.getFormat();
        final boolean uploadDriver = settings.uploadDriver();

        if (isDeprecatedNode) {
            exec.setMessage("Creating a Spark context...");
            ensureContextIsOpen(contextID, exec.createSubProgress(0.1));
        }

        final JobWithFilesRunFactory<GenericDataSource2SparkJobInput, GenericDataSource2SparkJobOutput> runFactory = SparkContextUtil.getJobWithFilesRunFactory(contextID, JOB_ID);

        final String namedOutputObject = SparkIDs.createSparkDataObjectID();
        LOGGER.info("Loading " + settings.getInputPath() + " into " + namedOutputObject + " rdd");
        final GenericDataSource2SparkJobInput jobInput;

        if (connectionInfo instanceof CloudConnectionInformation) {
            try {
                URI inUri = URIUtil.fromString(settings.getInputPath());
                URI knimeUri = connectionInfo.toURI().resolve(inUri);
                ConnectionMonitor<? extends Connection> connectionMonitor = new ConnectionMonitor<Connection>();
                RemoteFile<? extends Connection> remoteFile = RemoteFileFactory.createRemoteFile(knimeUri, connectionInfo, connectionMonitor);
                CloudRemoteFile<?> cloudRemoteFile = (CloudRemoteFile<?>) remoteFile;
                final String inputPath = cloudRemoteFile.getHadoopFilesystemString();
                jobInput = new GenericDataSource2SparkJobInput(namedOutputObject, format, uploadDriver, inputPath);
            } catch(UnsupportedOperationException e) {
                throw new InvalidSettingsException("Unsupported remote file connection.");
            }

        } else {
            final String inputPath = settings.getInputPath();
            jobInput = new GenericDataSource2SparkJobInput(namedOutputObject, format, uploadDriver, inputPath);
        }

        settings.addReaderOptions(jobInput);

        final List<File> toUpload = new ArrayList<>();
        if (jobInput.uploadDriver()) {
            toUpload.addAll(BundleGroupSparkJarRegistry
                .getBundledDriverJars(SparkContextUtil.getSparkVersion(contextID), jobInput.getFormat()));
        }

        try {
            final GenericDataSource2SparkJobOutput jobOutput;
            if (exec != null) {
                jobOutput = runFactory.createRun(jobInput, toUpload).run(contextID, exec);
            } else {
                jobOutput = runFactory.createRun(jobInput, toUpload).run(contextID);
            }

            final DataTableSpec outputSpec =
                KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(namedOutputObject));
            return new SparkDataTable(contextID, namedOutputObject, outputSpec);
        } catch (KNIMESparkException e) {
            final String message = e.getMessage();
            if (message != null && message.contains("Failed to find data source:")) {
                LOGGER.debug("Required data source driver not found in cluster. Original error message: "
            + e.getMessage());
                throw new InvalidSettingsException("Required datas source driver not found. Enable the 'Upload data source driver' "
                    + "option in the node dialog to upload the required driver files to the cluster.");
            }
            throw e;
        }
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedSettingsFrom(settings);
    }
}