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
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer;

import static org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.Spark2GenericDataSourceNodeFactory3.FS_INPUT_PORT_GRP_NAME;
import static org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.Spark2GenericDataSourceNodeFactory3.SPARK_INPUT_PORT_GRP_NAME;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Consumer;

import org.eclipse.core.runtime.URIUtil;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.jar.bundle.BundleGroupSparkJarRegistry;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.JobWithFilesRunFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.FileSystemPortChecker;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceJobInput;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;
import org.knime.filehandling.core.connections.uriexport.noconfig.NoConfigURIExporterFactory;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.WritePathAccessor;
import org.knime.filehandling.core.defaultnodesettings.status.NodeModelStatusConsumer;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage.MessageType;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;


/**
 * Spark to generic node model using NIO file system connections.
 *
 * @author Sascha Wolke, KNIME.com
 * @param <T> Settings type of this node
 */
public class Spark2GenericDataSourceNodeModel3<T extends Spark2GenericDataSourceSettings3> extends SparkNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Spark2GenericDataSourceNodeModel3.class);

    private final int m_fsInputPortIndex;

    private final int m_sparkInputPortIndex;

    /** The unique Spark job id. */
    public static final String JOB_ID = Spark2GenericDataSourceNodeModel.JOB_ID;

    /** Internal settings object. */
    private final T m_settings;

    private final NodeModelStatusConsumer m_statusConsumer;

    /**
     * Default Constructor
     *
     * @param portsConfig current port configuration
     * @param settings - Initial settings
     */
    public Spark2GenericDataSourceNodeModel3(final PortsConfiguration portsConfig, final T settings) {
        super(portsConfig.getInputPorts(), portsConfig.getOutputPorts());
        m_settings = settings;
        m_fsInputPortIndex = portsConfig.getInputPortLocation().get(FS_INPUT_PORT_GRP_NAME)[0];
        m_sparkInputPortIndex = portsConfig.getInputPortLocation().get(SPARK_INPUT_PORT_GRP_NAME)[0];
        m_statusConsumer = new NodeModelStatusConsumer(EnumSet.of(MessageType.ERROR, MessageType.WARNING));
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("File system or Spark data  input missing");
        }

        final FileSystemPortObjectSpec spec = (FileSystemPortObjectSpec)inSpecs[0];
        FileSystemPortChecker.checkFileSystemPortHadoopCompatibility(spec);

        m_settings.getFileChooserModel().configureInModel(inSpecs, m_statusConsumer);
        m_statusConsumer.setWarningsIfRequired(createWarningMessageConsumer());

        final SparkDataPortObjectSpec dataPortObjectSpec = (SparkDataPortObjectSpec)inSpecs[m_sparkInputPortIndex];
        final DataTableSpec tableSpec = dataPortObjectSpec.getTableSpec();

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

    @SuppressWarnings("resource") // unclosable connection
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");

        final String format = m_settings.getFormat();
        final String saveMode = m_settings.getSaveMode();
        final boolean uploadDriver = m_settings.uploadDriver();

        final FileSystemPortObject fsPort = (FileSystemPortObject)inData[m_fsInputPortIndex];

        final SparkDataPortObject sparkData = (SparkDataPortObject)inData[m_sparkInputPortIndex];
        final IntermediateSpec schema = SparkDataTableUtil.toIntermediateSpec(sparkData.getTableSpec());

        final JobWithFilesRunFactory<Spark2GenericDataSourceJobInput, EmptyJobOutput> runFactory =
            SparkContextUtil.getJobWithFilesRunFactory(sparkData.getContextID(), JOB_ID);
        final Spark2GenericDataSourceJobInput jobInput;

        try (final WritePathAccessor accessor = m_settings.getFileChooserModel().createWritePathAccessor()) {
            final FSPath outputFSPath = accessor.getOutputPath(m_statusConsumer);
            checkOutputPath(outputFSPath);
            final FSConnection fsConnection = fsPort.getFileSystemConnection().get(); // NOSONAR present check done in getOutputPath
            final URI clusterOutputURI = convertPathToURI(fsConnection, outputFSPath);
            final String clusterOutputPath = URIUtil.toUnencodedString(clusterOutputURI);
            jobInput = new Spark2GenericDataSourceJobInput(sparkData.getData().getID(), format, uploadDriver,
                clusterOutputPath, schema, saveMode);
        }

        addPartitioning(sparkData.getTableSpec(), jobInput);
        m_settings.addWriterOptions(jobInput);

        final List<File> toUpload = new ArrayList<>();
        if (jobInput.uploadDriver()) {
            toUpload.addAll(BundleGroupSparkJarRegistry
                .getBundledDriverJars(SparkContextUtil.getSparkVersion(sparkData.getContextID()), jobInput.getFormat()));
        }

        try {
            LOGGER.info("Writing Spark data " + sparkData.getData().getID() + " into '" + jobInput.getOutputPath() + "'.");
            runFactory.createRun(jobInput, toUpload).run(sparkData.getContextID(), exec);
        } catch (KNIMESparkException e) {
            final String message = e.getMessage();
            if (message != null && message.contains("Failed to find data source:")) {
                LOGGER.debug(
                    "Required data source driver not found in cluster. Original error message: " + e.getMessage());
                throw new InvalidSettingsException(
                    "Required data source driver not found. Enable the 'Upload data source driver' "
                        + "option in the node dialog to upload the required driver files to the cluster.");
            }
            throw e;
        }

        return new PortObject[0];
    }

    private Consumer<String> createWarningMessageConsumer() {
        return new Consumer<String>() {
            @Override
            public void accept(final String warningMessage) {
                setWarningMessage(warningMessage);
            }
        };
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

    private void checkOutputPath(final Path outputPath) throws IOException {
        final Path parentPath = outputPath.toAbsolutePath().getParent();

        if (parentPath != null && !FSFiles.exists(parentPath)) {
            if (m_settings.getFileChooserModel().isCreateMissingFolders()) {
                FSFiles.createDirectories(parentPath);
            } else {
                throw new IOException(String.format(
                    "The directory '%s' does not exist and must not be created due to user settings.", parentPath));
            }

        } else if (parentPath != null && !FSFiles.isDirectory(parentPath)) {
            throw new IOException(String.format("The parent output path '%s' must be a directory.", outputPath));

        } else if (m_settings.isAppendMode() && FSFiles.exists(outputPath) && !FSFiles.isDirectory(outputPath)) { // NOSONAR no else block
            throw new IOException(
                String.format("The output path '%s' must be a directory in append mode.", outputPath));
        }
    }

    private static URI convertPathToURI(final FSConnection fsConnection, final FSPath fsPath)
        throws URISyntaxException {
        final NoConfigURIExporterFactory uriExporterFactory =
                (NoConfigURIExporterFactory)fsConnection.getURIExporterFactories().get(URIExporterIDs.DEFAULT_HADOOP);
            final URIExporter uriExporter = uriExporterFactory.getExporter();
            final URI uri = uriExporter.toUri(fsPath);
        return uri;
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsToModel(settings);
    }

    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedSettingsFromModel(settings);
    }
}
