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
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Consumer;

import org.eclipse.core.runtime.URIUtil;
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
import org.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobInput;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobOutput;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.ReadPathAccessor;
import org.knime.filehandling.core.defaultnodesettings.status.NodeModelStatusConsumer;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage.MessageType;
import org.knime.filehandling.core.port.FileSystemPortObject;

/**
 * Generic to Spark node model using NIO file system connections.
 *
 * @author Sascha Wolke, KNIME.com
 * @param <T> Settings type.
 */
public class GenericDataSource2SparkNodeModel3<T extends GenericDataSource2SparkSettings3> extends SparkSourceNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(GenericDataSource2SparkNodeModel3.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = GenericDataSource2SparkNodeModel.JOB_ID;

    /** Internal settings object. */
    private final T m_settings;

    private final NodeModelStatusConsumer m_statusConsumer;

    /**
     * Default constructor.
     *
     * @param portsConfig current port configuration
     * @param settings - Initial settings
     */
    public GenericDataSource2SparkNodeModel3(final PortsConfiguration portsConfig, final T settings) {
        super(new PortType[]{FileSystemPortObject.TYPE}, false, new PortType[]{SparkDataPortObject.TYPE});
        m_settings = settings;
        m_statusConsumer = new NodeModelStatusConsumer(EnumSet.of(MessageType.ERROR, MessageType.WARNING));
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("File system or Spark data input missing");
        }

        m_settings.getFileChooserModel().configureInModel(inSpecs, m_statusConsumer);
        m_statusConsumer.setWarningsIfRequired(createWarningMessageConsumer());
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
        final String inputPath = getInputSparkPath(m_settings, m_statusConsumer);
        final SparkDataTable resultTable = runJob(m_settings, getContextID(inData), inputPath, exec);
        final SparkDataPortObject sparkObject = new SparkDataPortObject(resultTable);
        return new PortObject[] { sparkObject };
    }

    /**
     * Run job with given settings for preview table.
     * @param settings - Settings to use.
     * @param contextID - Context to run on.
     * @param statusConsumer - for communicating non-fatal errors and warnings
     * @param exec - Execution monitor to use.
     * @return Result table
     * @throws Exception
     */
    public static <T extends GenericDataSource2SparkSettings3> SparkDataTable preparePreview(final T settings, final SparkContextID contextID,
        final ExecutionMonitor exec, final Consumer<StatusMessage> statusConsumer) throws Exception {

        exec.setMessage("Running spark job and fetching preview");
        return runJob(settings, contextID, getInputSparkPath(settings, statusConsumer), exec);
    }

    @SuppressWarnings("resource") // unclosable connection
    private static <T extends GenericDataSource2SparkSettings3> String getInputSparkPath(final T settings,
        final Consumer<StatusMessage> statusConsumer) throws URISyntaxException, IOException, InvalidSettingsException {

        try (final ReadPathAccessor accessor = settings.getFileChooserModel().createReadPathAccessor()) {
            final FSPath inputFSPath = accessor.getRootPath(statusConsumer);
            final FSConnection fsConnection = settings.getFileChooserModel().getConnection();
            final URI clusterInputURI = fsConnection.getDefaultURIExporter().toUri(inputFSPath);
            return URIUtil.toUnencodedString(clusterInputURI);
        }
    }

    private static <T extends GenericDataSource2SparkSettings3> SparkDataTable runJob(final T settings,
        final SparkContextID contextID, final String inputPath,
        final ExecutionMonitor exec) throws Exception {

        final String format = settings.getFormat();
        final boolean uploadDriver = settings.uploadDriver();

        final JobWithFilesRunFactory<GenericDataSource2SparkJobInput, GenericDataSource2SparkJobOutput> runFactory = SparkContextUtil.getJobWithFilesRunFactory(contextID, JOB_ID);

        final String namedOutputObject = SparkIDs.createSparkDataObjectID();
        LOGGER.info("Loading " + inputPath + " into " + namedOutputObject + " rdd");
        final GenericDataSource2SparkJobInput jobInput =
            new GenericDataSource2SparkJobInput(namedOutputObject, format, uploadDriver, inputPath);
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

    private Consumer<String> createWarningMessageConsumer() {
        return new Consumer<String>() {
            @Override
            public void accept(final String warningMessage) {
                setWarningMessage(warningMessage);
            }
        };
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