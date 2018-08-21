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
package org.knime.bigdata.spark.node.io.genericdatasource.writer;

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
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.jar.bundle.BundleGroupSparkJarRegistry;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.JobWithFilesRunFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * @author Sascha Wolke, KNIME.com
 * @param <T> Settings type of this node
 */
public class Spark2GenericDataSourceNodeModel<T extends Spark2GenericDataSourceSettings> extends SparkNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Spark2GenericDataSourceNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = Spark2GenericDataSourceNodeModel.class.getCanonicalName();

    /** Internal settings object. */
    private final Spark2GenericDataSourceSettings m_settings;

    /**
     * Default Constructor
     *
     * @param settings - Initial settings
     */
    public Spark2GenericDataSourceNodeModel(final Spark2GenericDataSourceSettings settings) {
        super(new PortType[]{ConnectionInformationPortObject.TYPE, SparkDataPortObject.TYPE}, new PortType[0]);

        m_settings = settings;
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("Connection or input data missing");
        }

        final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec)inSpecs[0];
        final ConnectionInformation connInfo = object.getConnectionInformation();
        final SparkDataPortObjectSpec dataPortObjectSpec = (SparkDataPortObjectSpec)inSpecs[1];
        final DataTableSpec tableSpec = dataPortObjectSpec.getTableSpec();

        if (connInfo == null) {
            throw new InvalidSettingsException("No connection information available");
        }

        if (!HDFSRemoteFileHandler.isSupportedConnection(connInfo) && !(connInfo instanceof CloudConnectionInformation)
            && !(RemoteFileHandlerRegistry.getProtocol(connInfo.getProtocol()).getName().contains("hdfs"))) {
            throw new InvalidSettingsException("HDFS or cloud connection required");
        }

        m_settings.loadDefault(tableSpec);
        m_settings.validateSettings();

        final SparkVersion version = SparkContextUtil.getSparkVersion(dataPortObjectSpec.getContextID());
        if (!m_settings.isCompatibleSparkVersion(version)) {
            throw new InvalidSettingsException("Unsupported Spark Version! This node requires at least Spark "
                + m_settings.getMinSparkVersion() + ".");
        }

        if (m_settings.supportsPartitioning()) {
            String[] partitions = m_settings.getPartitionBy().applyTo(tableSpec).getIncludes();
            if (partitions.length > 0) {
                for (String colName : partitions) {
                    if (tableSpec.getColumnSpec(colName) == null) {
                        throw new InvalidSettingsException(
                            "Partitioning column '" + colName + "' does not exist in input RDD");
                    }
                }
            }

            // check that at least one non partitioned column exists
            if (tableSpec.getNumColumns() == partitions.length) {
                throw new InvalidSettingsException("Cannot use all columns for partitioning.");
            }
        }

        return new PortObjectSpec[0];
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");

        final ConnectionInformationPortObject object = (ConnectionInformationPortObject)inData[0];
        final ConnectionInformation connectionInfo = object.getConnectionInformation();

        final String format = m_settings.getFormat();
        final String outputPath = m_settings.getDirectory() + "/" + m_settings.getName();
        final String saveMode = m_settings.getSaveMode();
        final boolean uploadDriver = m_settings.uploadDriver();

        final SparkDataPortObject rdd = (SparkDataPortObject)inData[1];
        final IntermediateSpec schema = SparkDataTableUtil.toIntermediateSpec(rdd.getTableSpec());

        LOGGER.info("Writing " + rdd.getData().getID() + " rdd into " + outputPath);
        final JobWithFilesRunFactory<Spark2GenericDataSourceJobInput, EmptyJobOutput> runFactory =
            SparkContextUtil.getJobWithFilesRunFactory(rdd.getContextID(), JOB_ID);
        final Spark2GenericDataSourceJobInput jobInput;

        if (connectionInfo instanceof CloudConnectionInformation) {
            try {
                URI outURI = URIUtil.fromString(outputPath);
                URI knimeUri = connectionInfo.toURI().resolve(outURI);
                ConnectionMonitor<? extends Connection> connectionMonitor = new ConnectionMonitor<Connection>();
                RemoteFile<? extends Connection> remoteFile =
                    RemoteFileFactory.createRemoteFile(knimeUri, connectionInfo, connectionMonitor);
                CloudRemoteFile<?> cloudRemoteFile = (CloudRemoteFile<?>)remoteFile;
                final String clusterOutputPath = cloudRemoteFile.getHadoopFilesystemString();
                jobInput = new Spark2GenericDataSourceJobInput(rdd.getData().getID(), format, uploadDriver,
                    clusterOutputPath, false, schema, saveMode);
            } catch (UnsupportedOperationException e) {
                throw new InvalidSettingsException("Unsupported remote file connection.");
            }

        } else {
            jobInput = new Spark2GenericDataSourceJobInput(rdd.getData().getID(), format, uploadDriver, outputPath,
                true, schema, saveMode);
        }

        addPartitioning(rdd.getTableSpec(), jobInput);
        m_settings.addWriterOptions(jobInput);

        final List<File> toUpload = new ArrayList<>();
        if (jobInput.uploadDriver()) {
            toUpload.addAll(BundleGroupSparkJarRegistry
                .getBundledDriverJars(SparkContextUtil.getSparkVersion(rdd.getContextID()), jobInput.getFormat()));
        }

        try {
            runFactory.createRun(jobInput, toUpload).run(rdd.getContextID(), exec);
        } catch (KNIMESparkException e) {
            final String message = e.getMessage();
            if (message != null && message.contains("Failed to find data source:")) {
                LOGGER.debug(
                    "Required data source driver not found in cluster. Original error message: " + e.getMessage());
                throw new InvalidSettingsException(
                    "Required datas source driver not found. Enable the 'Upload data source driver' "
                        + "option in the node dialog to upload the required driver files to the cluster.");
            }
            throw e;
        }

        return new PortObject[0];
    }

    /** Add partitioning columns and number to job input if present in settings */
    private void addPartitioning(final DataTableSpec tableSpec, final Spark2GenericDataSourceJobInput jobInput) {
        if (m_settings.supportsPartitioning()) {
            String partitions[] = m_settings.getPartitionBy().applyTo(tableSpec).getIncludes();
            if (partitions.length > 0) {
                jobInput.setPartitioningBy(partitions);
            }
        }

        if (m_settings.overwriteNumPartitions()) {
            jobInput.setNumPartitions(m_settings.getNumPartitions());
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
