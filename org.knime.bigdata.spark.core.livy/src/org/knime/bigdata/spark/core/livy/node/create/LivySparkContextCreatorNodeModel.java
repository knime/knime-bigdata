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

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.livy.context.LivySparkContext;
import org.knime.bigdata.spark.core.livy.context.LivySparkContextConfig;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 * Node model of the "Create Spark Context (Livy)" node using a {@link ConnectionInformation} based file system.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivySparkContextCreatorNodeModel extends AbstractLivySparkContextCreatorNodeModel {

    /**
     * Default constructor.
     */
    LivySparkContextCreatorNodeModel() {
        super(new PortType[]{ConnectionInformationPortObject.TYPE}, new PortType[]{SparkContextPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Remote file handling connection missing");
        }

        final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec) inSpecs[0];
        final ConnectionInformation connInfo = object.getConnectionInformation();
        if (connInfo == null) {
            throw new InvalidSettingsException("No remote file handling connection information available");
        }

        if (connInfo instanceof CloudConnectionInformation && !m_settings.isStagingAreaFolderSet()) {
            throw new InvalidSettingsException(
                String.format("When connecting to %s a staging directory must be specified (see Advanced tab).",
                    ((CloudConnectionInformation)connInfo).getServiceName()));
        }

        m_settings.validateDeeper();
        configureSparkContext(m_sparkContextId, connInfo, m_settings, getCredentialsProvider());
        return new PortObjectSpec[]{new SparkContextPortObjectSpec(m_sparkContextId)};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final ConnectionInformationPortObject object = (ConnectionInformationPortObject) inData[0];
        final ConnectionInformation connInfo = object.getConnectionInformation();

        exec.setProgress(0, "Configuring Livy Spark context");
        configureSparkContext(m_sparkContextId, connInfo, m_settings, getCredentialsProvider());

        final LivySparkContext sparkContext =
            (LivySparkContext)SparkContextManager.<LivySparkContextConfig> getOrCreateSparkContext(m_sparkContextId);

        // try to open the context
        exec.setProgress(0.1, "Creating context");
        sparkContext.ensureOpened(true, exec.createSubProgress(0.9));

        return new PortObject[]{new SparkContextPortObject(m_sparkContextId)};
    }

    @Override
    protected void createDummyContext(final String previousContextID) {
        final ConnectionInformation dummyConnInfo = new ConnectionInformation();
        configureSparkContext(new SparkContextID(previousContextID), dummyConnInfo, m_settings,
            getCredentialsProvider());
    }

    /**
     * Internal method to ensure that the given Spark context is configured.
     *
     * @param sparkContextId Identifies the Spark context to configure.
     * @param connInfo
     * @param settings The settings from which to configure the context.
     * @param credProv Credentials provider to use
     */
    protected static void configureSparkContext(final SparkContextID sparkContextId, final ConnectionInformation connInfo,
        final LivySparkContextCreatorNodeSettings settings, final CredentialsProvider credProv) {

        final SparkContext<LivySparkContextConfig> sparkContext =
            SparkContextManager.getOrCreateSparkContext(sparkContextId);
        final LivySparkContextConfig config = settings.createContextConfig(sparkContextId, connInfo, credProv);

        final boolean configApplied = sparkContext.ensureConfigured(config, true);
        if (!configApplied) {
            // this should never ever happen
            throw new RuntimeException("Failed to apply Spark context settings.");
        }
    }

}
