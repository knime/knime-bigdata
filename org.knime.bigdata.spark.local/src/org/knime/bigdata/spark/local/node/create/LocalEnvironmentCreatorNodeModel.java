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
package org.knime.bigdata.spark.local.node.create;

import java.io.File;
import java.io.IOException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.base.node.io.database.connection.util.ParameterizedDatabaseConnectionSettings;
import org.knime.bigdata.filehandling.local.HDFSLocalConnectionInformation;
import org.knime.bigdata.filehandling.local.HDFSLocalRemoteFileHandler;
import org.knime.bigdata.hive.utility.HiveDriverDetector;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContext.SparkContextStatus;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
import org.knime.bigdata.spark.local.context.LocalSparkContext;
import org.knime.bigdata.spark.local.context.LocalSparkContextConfig;
import org.knime.bigdata.spark.local.database.LocalHiveUtility;
import org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings.OnDisposeAction;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.inactive.InactiveBranchPortObject;
import org.knime.core.node.port.inactive.InactiveBranchPortObjectSpec;

/**
 * Node model for the "Create Local Big Data Environment" node.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LocalEnvironmentCreatorNodeModel extends SparkNodeModel {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(LocalEnvironmentCreatorNodeModel.class);

	private final LocalSparkContextSettings m_settings = new LocalSparkContextSettings();

	private SparkContextID m_lastContextID;

	/**
	 * Constructor.
	 */
	LocalEnvironmentCreatorNodeModel() {
		super(new PortType[]{}, new PortType[]{DatabaseConnectionPortObject.TYPE,
				ConnectionInformationPortObject.TYPE, SparkContextPortObject.TYPE});
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		final SparkContextID newContextID = m_settings.getSparkContextID();
		final SparkContext<LocalSparkContextConfig> sparkContext = SparkContextManager.getOrCreateSparkContext(newContextID);
		final LocalSparkContextConfig config = m_settings.createContextConfig();

		m_settings.validateDeeper();

		if (m_lastContextID != null && !m_lastContextID.equals(newContextID)
				&& SparkContextManager.getOrCreateSparkContext(m_lastContextID).getStatus() == SparkContextStatus.OPEN) {
			LOGGER.warn("Context ID has changed. Keeping old local Spark context alive and configuring new one!");
		}

		final boolean configApplied = sparkContext.ensureConfigured(config, true);
		if (!configApplied && !m_settings.hideExistsWarning()) {
			// this means context was OPEN and we are changing settings that cannot become active without
			// destroying and recreating the remote context. Furthermore the node settings say that
			// there should be a warning about this situation
			setWarningMessage("Local Spark context exists already. Settings were not applied.");
		}

		m_lastContextID = newContextID;

		PortObjectSpec dbPortObjectObjectSpec = null;
		if (!sparkContext.getConfiguration().startThriftserver()) {
			dbPortObjectObjectSpec = InactiveBranchPortObjectSpec.INSTANCE;
		}

		return new PortObjectSpec[]{dbPortObjectObjectSpec,
				createHDFSConnectionSpec(), new SparkContextPortObjectSpec(m_settings.getSparkContextID())};
	}



	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {

		final SparkContextID contextID = m_settings.getSparkContextID();
		final LocalSparkContext sparkContext = (LocalSparkContext) SparkContextManager
				.<LocalSparkContextConfig>getOrCreateSparkContext(contextID);

		exec.setProgress(0, "Configuring local Spark context");
		final LocalSparkContextConfig config = m_settings.createContextConfig();

		final boolean configApplied = sparkContext.ensureConfigured(config, true);
		if (!configApplied && !m_settings.hideExistsWarning()) {
			// this means context was OPEN and we are changing settings that cannot become active without
			// destroying and recreating the remote context. Furthermore the node settings say that
			// there should be a warning about this situation
			setWarningMessage("Local Spark context exists already. Settings were not applied.");
		}

		// try to open the context
		exec.setProgress(0.1, "Creating context");
		sparkContext.ensureOpened(true, exec.createSubProgress(0.9));

		final PortObject dbPortObject;
		if (sparkContext.getConfiguration().startThriftserver()) {
			final int hiveserverPort = sparkContext.getHiveserverPort();
			dbPortObject = new DatabaseConnectionPortObject(getHiveSpec(hiveserverPort));
		} else {
			dbPortObject = InactiveBranchPortObject.INSTANCE;
		}

		return new PortObject[]{dbPortObject,
				new ConnectionInformationPortObject(createHDFSConnectionSpec()),
				new SparkContextPortObject(contextID)};
	}

	private static DatabaseConnectionPortObjectSpec getHiveSpec(final int hiveserverPort) {
		final ParameterizedDatabaseConnectionSettings connSettings = new ParameterizedDatabaseConnectionSettings();

		connSettings.setDatabaseIdentifier(LocalHiveUtility.DATABASE_IDENTIFIER);
		connSettings.setDriver(HiveDriverDetector.getDriverName());
		connSettings.setPort(hiveserverPort);
		connSettings.setRowIdsStartWithZero(true);
		connSettings.setRetrieveMetadataInConfigure(false);
		connSettings.setHost("localhost");
		connSettings.setParameter("");
		connSettings.setDatabaseName("");
		connSettings.setJDBCUrl(getJDBCURL(connSettings.getHost(),
				connSettings.getPort(), connSettings.getDatabaseName(), connSettings.getParameter()));
		final DatabaseConnectionPortObjectSpec spec = new DatabaseConnectionPortObjectSpec(connSettings);

		return spec;
	}

	private static String getJDBCURL(final String host, final int port, final String databaseName, final String parameter) {
		final StringBuilder buf = new StringBuilder("jdbc:hive2://" + host + ":" + port);
		//append database
		buf.append("/" + databaseName);
		if (parameter != null && !parameter.trim().isEmpty()) {
			if (!parameter.startsWith(";")) {
				buf.append(";");
			}
			buf.append(parameter);
		}
		final String jdbcUrl = buf.toString();
		LOGGER.debug("Using jdbc url: " + jdbcUrl);
		return jdbcUrl;
	}

	/**
	 * Create the spec for the local hdfs.
	 *
	 * @return ...
	 * @throws InvalidSettingsException ...
	 */
	private static ConnectionInformationPortObjectSpec createHDFSConnectionSpec() throws InvalidSettingsException {
		final HDFSLocalConnectionInformation connectionInformation = new HDFSLocalConnectionInformation();
		final Protocol protocol = HDFSLocalRemoteFileHandler.HDFS_LOCAL_PROTOCOL;
		connectionInformation.setProtocol(protocol.getName());
		connectionInformation.setHost("localhost");
		connectionInformation.setPort(protocol.getPort());
		connectionInformation.setUser(null);
		connectionInformation.setPassword(null);
		return new ConnectionInformationPortObjectSpec(connectionInformation);
	}

	@Override
	protected void onDisposeInternal() {
		if (m_settings.getOnDisposeAction() == OnDisposeAction.DESTROY_CTX) {
			final SparkContextID id = m_settings.getSparkContextID();

			try {
				SparkContextManager.ensureSparkContextDestroyed(id);
			} catch (final KNIMESparkException e) {
				LOGGER.debug("Failed to destroy context " + id + " on dispose.", e);
			}
		}
	}

	@Override
	protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {

		final SparkContextID contextID = m_settings.getSparkContextID();
		final SparkContext<LocalSparkContextConfig> sparkContext = SparkContextManager.getOrCreateSparkContext(contextID);

		final LocalSparkContextConfig sparkContextConfig = m_settings.createContextConfig();

		final boolean configApplied = sparkContext.ensureConfigured(sparkContextConfig, true);
		if (!configApplied && !m_settings.hideExistsWarning()) {
			setWarningMessage("Local Spark context exists already. Settings were not applied.");
		}
	}



	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
		m_settings.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_settings.validateSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_settings.loadSettingsFrom(settings);
	}
}
