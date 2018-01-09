/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.bigdata.scripting.nodes.python.hive;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.node.util.exttool.ExtToolOutputNodeModel;
import org.knime.bigdata.hive.utility.HiveLoader;
import org.knime.bigdata.hive.utility.HiveLoaderSettings;
import org.knime.bigdata.hive.utility.HiveUtility;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseReaderConnection;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.python.kernel.PythonKernel;
import org.knime.python.kernel.SQLEditorObjectReader;
import org.knime.python.kernel.SQLEditorObjectWriter;

/**
 * This is the model implementation.
 *
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
class PythonScriptHiveNodeModel extends ExtToolOutputNodeModel {

	private PythonScriptHiveNodeConfig m_config = new PythonScriptHiveNodeConfig();

	static final String CFG_TARGET_FOLDER = "targetFolder";

	/**
	 * Constructor for the node model.
	 */
	protected PythonScriptHiveNodeModel() {
		super(new PortType[] {ConnectionInformationPortObject.TYPE, DatabasePortObject.TYPE}, new PortType[] {DatabasePortObject.TYPE});
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
		final PythonKernel kernel = new PythonKernel();
		final ConnectionInformation fileInfo = ((ConnectionInformationPortObject)inData[0]).getConnectionInformation();
		if (fileInfo == null) {
            throw new InvalidSettingsException("No remote file port available");
        }
		final DatabasePortObject dbObj = (DatabasePortObject) inData[1];
		checkDBConnection(dbObj.getSpec());
		try {
			kernel.putFlowVariables(PythonScriptHiveNodeConfig.getVariableNames().getFlowVariables(),
					getAvailableFlowVariables().values());
			final CredentialsProvider cp = getCredentialsProvider();
			final DatabaseQueryConnectionSettings connIn = dbObj.getConnectionSettings(cp);
			final Collection<String> jars = getJars(connIn);
			final SQLEditorObjectWriter sqlObject = new SQLEditorObjectWriter(
					PythonScriptHiveNodeConfig.getVariableNames().getGeneralInputObjects()[0], connIn, cp, jars);
			kernel.putGeneralObject(sqlObject);
			final String[] output = kernel.execute(m_config.getSourceCode(), exec);
			setExternalOutput(new LinkedList<>(Arrays.asList(output[0].split("\n"))));
			setExternalErrorOutput(new LinkedList<>(Arrays.asList(output[1].split("\n"))));
			exec.createSubProgress(0.2).setProgress(1);
			final SQLEditorObjectReader sqlReader = new SQLEditorObjectReader(
					PythonScriptHiveNodeConfig.getVariableNames().getGeneralInputObjects()[0]);
			kernel.getGeneralObject(sqlReader);
			final String targetFolder= m_config.getTargetFolder();
			uploadToHive(exec.createSubExecutionContext(0.8), fileInfo, connIn, cp, sqlReader, targetFolder);
			exec.setProgress("Determining table structure");
			final DatabaseQueryConnectionSettings connOut = new DatabaseQueryConnectionSettings(connIn, sqlReader.getQuery());
			final DatabaseReaderConnection dbCon = new DatabaseReaderConnection(connOut);
			final DataTableSpec resultSpec = dbCon.getDataTableSpec(cp);
			return new PortObject[] { new DatabasePortObject(new DatabasePortObjectSpec(resultSpec, connOut)) };
		} finally {
			kernel.close();
		}
	}

    static Collection<String> getJars(final DatabaseQueryConnectionSettings connIn) throws IOException {
        final Collection<File> driverFiles =
                connIn.getUtility().getConnectionFactory().getDriverFactory().getDriverFiles(connIn);
        final Collection<String> driverPaths = new LinkedList<>();
        for (File file : driverFiles) {
            driverPaths.add(file.getAbsolutePath());
        }
        return driverPaths;
    }

    @SuppressWarnings("unchecked")
    private void uploadToHive(final ExecutionContext exec, final ConnectionInformation connInfo,
        final DatabaseQueryConnectionSettings connIn, final CredentialsProvider cp,
        final SQLEditorObjectReader sqlReader, final String targetFolder) throws Exception {
        File dataFile = null;
        RemoteFile<? extends Connection> remoteFile = null;
        ConnectionMonitor<?> connMonitor = new ConnectionMonitor<>();
        try {
            dataFile = new File(sqlReader.getFileName());
            final HiveLoaderSettings settings = createHiveLoaderSettings(sqlReader, targetFolder);
            final HiveLoader hiveLoader = HiveLoader.getInstance();
            remoteFile = hiveLoader.uploadFile(dataFile, connInfo, (ConnectionMonitor<? extends Connection>)connMonitor, exec, settings);
            final List<String> columnNames = sqlReader.getColumnNamesList();
            hiveLoader.importData(remoteFile, columnNames, connIn, exec, settings, cp);
        } finally {
            Exception er = null;
            if (dataFile != null) {
                try {
                    if (!dataFile.delete()) {
                        setWarningMessage("Could not delete temporary import file on client");
                    }
                } catch (Exception e) {
                    er = e;
                }
            }
            if (remoteFile != null) {
                try {
                    if (!remoteFile.delete()) {
                        setWarningMessage("Could not delete temporary import file on server");
                    }
                } catch (Exception e) {
                    er = e;
                }
                connMonitor.closeAll();
                if (er != null) {
                    throw er;
                }
            }
        }
    }

    private HiveLoaderSettings createHiveLoaderSettings(final SQLEditorObjectReader reader, final String targetFolder) {
        HiveLoaderSettings settings = new HiveLoaderSettings();
        settings.dropTableIfExists(reader.isDropTable());
        settings.partitionColumn(reader.getPartitionColumnNamesList());
        settings.tableName(reader.getTableName());
        settings.valueDelimiter(reader.getDelimiter());
        settings.clearTypeMapping();
        List<String> columnNamesList = reader.getColumnNamesList();
        List<String> columnTypeList = reader.getColumnTypeList();
        if (columnNamesList.size() != columnTypeList.size()) {
            throw new IllegalArgumentException("Column names and types should have the same length");
        }
        for (int i = 0, length = columnNamesList.size(); i < length; i++) {
            settings.typeMapping(columnNamesList.get(i), columnTypeList.get(i));
        }
        settings.targetFolder(targetFolder);
        return settings;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		final ConnectionInformationPortObjectSpec fileSpec = (ConnectionInformationPortObjectSpec) inSpecs[0];
		if (fileSpec == null) {
		    throw new InvalidSettingsException("No remote file port available");
		}
		final DatabasePortObjectSpec dbSpec = (DatabasePortObjectSpec) inSpecs[1];
		checkDBConnection(dbSpec);
		return new PortObjectSpec[] { null };
	}

	/**
	 * @param spec
	 * @throws InvalidSettingsException
	 */
	private void checkDBConnection(final DatabasePortObjectSpec spec)
			throws InvalidSettingsException {
		final String dbIdentifier = spec.getDatabaseIdentifier();
		if (!HiveUtility.DATABASE_IDENTIFIER.equals(dbIdentifier)) {
		    throw new InvalidSettingsException("Only Hive connections are supported");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		m_config.saveTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		final PythonScriptHiveNodeConfig config = new PythonScriptHiveNodeConfig();
		config.loadFrom(settings);
		String targetFolder = config.getTargetFolder();
        if (targetFolder == null || targetFolder.trim().isEmpty()) {
            throw new InvalidSettingsException("Please specify the target folder");
        }
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		final PythonScriptHiveNodeConfig config = new PythonScriptHiveNodeConfig();
		config.loadFrom(settings);
		m_config = config;
	}

}
