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
 *   Created on 03.07.2015 by koetter
 */
package org.knime.bigdata.spark.core.livy.node.create;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.livy.context.LivySparkContext;
import org.knime.bigdata.spark.core.livy.context.LivySparkContextConfig;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Node model of the "Create Spark Context (Livy)" node using a NIO file system.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivySparkContextCreatorNodeModel2 extends AbstractLivySparkContextCreatorNodeModel {

    /**
     * Default Constructor.
     */
    LivySparkContextCreatorNodeModel2() {
        super(new PortType[]{FileSystemPortObject.TYPE}, new PortType[]{SparkContextPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("File system connection missing");
        }

        final FileSystemPortObjectSpec spec = (FileSystemPortObjectSpec)inSpecs[0];
        m_settings.validateDeeper();

        configureSparkContext(m_sparkContextId, spec.getFileSystemId(), m_settings, getCredentialsProvider());
        return new PortObjectSpec[]{new SparkContextPortObjectSpec(m_sparkContextId)};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final FileSystemPortObject fsPort = (FileSystemPortObject) inData[0];
        final String fileSystemId = fsPort.getSpec().getFileSystemId();

        exec.setProgress(0, "Configuring Livy Spark context");
        configureSparkContext(m_sparkContextId, fileSystemId, m_settings, getCredentialsProvider());

        final LivySparkContext sparkContext =
            (LivySparkContext)SparkContextManager.<LivySparkContextConfig> getOrCreateSparkContext(m_sparkContextId);

        // try to open the context
        exec.setProgress(0.1, "Creating context");
        sparkContext.ensureOpened(true, exec.createSubProgress(0.9));

        return new PortObject[]{new SparkContextPortObject(m_sparkContextId)};
    }

    @Override
    protected void createDummyContext(final String previousContextID) {
        final String dummyFileSystemId = "dummy-file-system-id";
        configureSparkContext(new SparkContextID(previousContextID), dummyFileSystemId, m_settings,
            getCredentialsProvider());
    }

    /**
     * Internal method to ensure that the given Spark context is configured.
     *
     * @param sparkContextId Identifies the Spark context to configure.
     * @param fileSystemId Identifies the staging area file system connection.
     * @param settings The settings from which to configure the context.
     * @param credProv Credentials provider to use
     */
    protected static void configureSparkContext(final SparkContextID sparkContextId, final String fileSystemId,
        final LivySparkContextCreatorNodeSettings settings, final CredentialsProvider credProv) {

        final SparkContext<LivySparkContextConfig> sparkContext =
            SparkContextManager.getOrCreateSparkContext(sparkContextId);
        final LivySparkContextConfig config = settings.createContextConfig(sparkContextId, fileSystemId, credProv);

        final boolean configApplied = sparkContext.ensureConfigured(config, true);
        if (!configApplied) {
            // this should never ever happen
            throw new RuntimeException("Failed to apply Spark context settings.");
        }
    }

}
