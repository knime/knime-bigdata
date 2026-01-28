/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 *
 * History
 *   2024-05-24 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.databricks.unity.filehandling.node;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSConnection;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSConnectionConfig;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFileSystem;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FSConnectionProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.WithCustomFileSystem;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.CustomValidation;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.SimpleValidation;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.LoadDefaultsForAbsentFields;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;

/**
 * Node parameters for the Databricks Unity File System Connector.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@LoadDefaultsForAbsentFields
@SuppressWarnings("restriction")
public class UnityFileSystemConnectorNodeParameters implements NodeParameters {

    @Section(title = "File System")
    interface FileSystemSection {
    }

    @Layout(FileSystemSection.class)
    @Widget(title = "Working directory", //
        description = "Specifies the <i>working directory</i> of the resulting file system connection."
            + " The working directory must be specified as an absolute path."
            + " A working directory allows downstream nodes to access files/folders using <i>relative</i> paths,"
            + " i.e. paths that do not have a leading slash."
            + " If not specified, the default working directory is \"/\".")
    @FileSelectionWidget
    @WithCustomFileSystem(
        connectionProvider = UnityFileSystemConnectorNodeParameters.FileSystemConnectionProvider.class)
    @ValueReference(WorkingDirectoryRef.class)
    @CustomValidation(WorkingDirectoryValidator.class)
    @Persist(configKey = "workingDirectory")
    String m_workingDirectory = "/";

    interface WorkingDirectoryRef extends ParameterReference<String> {
    }

    static class WorkingDirectoryValidator extends SimpleValidation<String> {

        @Override
        public void validate(final String currentValue) throws InvalidSettingsException {
            if (StringUtils.isBlank(currentValue)) {
                throw new InvalidSettingsException("Please specify a working directory.");
            } else if (!currentValue.startsWith(UnityFileSystem.PATH_SEPARATOR)) {
                throw new InvalidSettingsException("Working directory must be an absolute path that starts with \"/\"");
            }
        }
    }

    /**
     * Provides a {@link FSConnectionProvider} based on the Databricks Unity File System
     * connection settings. This enables the working directory field to have a file
     * system browser.
     */
    static final class FileSystemConnectionProvider implements StateProvider<FSConnectionProvider> {

        private Supplier<String> m_workingDirectorySupplier;

        private static final String ERROR_MSG =
            "Connection not available. Please re-execute the preceding connector node and make sure it is connected.";

        @Override
        public void init(final StateProvider.StateProviderInitializer initializer) {
            m_workingDirectorySupplier = initializer.computeFromValueSupplier(WorkingDirectoryRef.class);
            initializer.computeAfterOpenDialog();
        }

        @Override
        public FSConnectionProvider computeState(final NodeParametersInput parametersInput) {
            final PortObjectSpec[] inputSpecs = parametersInput.getInPortSpecs();
            final boolean hasWorkspaceConnection = inputSpecs != null && inputSpecs.length > 0
                && inputSpecs[0] instanceof DatabricksWorkspacePortObjectSpec;

            return () -> createConnection(hasWorkspaceConnection, inputSpecs);
        }

        private UnityFSConnection createConnection(final boolean hasWorkspaceConnection,
            final PortObjectSpec[] inputSpecs) throws InvalidSettingsException, IOException {
            final DatabricksWorkspacePortObjectSpec spec = getWorkspaceSpec(hasWorkspaceConnection, inputSpecs);
            final DatabricksAccessTokenCredential credential = resolveCredential(spec);
            final String workingDir = normalizeWorkingDirectory(m_workingDirectorySupplier.get());
            final UnityFSConnectionConfig config = UnityFSConnectionConfig.builder() //
                .withCredential(credential) //
                .withWorkingDirectory(workingDir) //
                .withConnectionTimeout(spec.getConnectionTimeout()) //
                .withReadTimeout(spec.getConnectionTimeout()) //
                .build();
            final UnityFSConnection connection = new UnityFSConnection(config);
            testConnection(connection);
            return connection;
        }

        private static DatabricksWorkspacePortObjectSpec getWorkspaceSpec(final boolean hasWorkspaceConnection,
            final PortObjectSpec[] inputSpecs) throws InvalidSettingsException {
            if (!hasWorkspaceConnection) {
                throw new InvalidSettingsException(ERROR_MSG);
            }

            final DatabricksWorkspacePortObjectSpec spec = (DatabricksWorkspacePortObjectSpec)inputSpecs[0];
            if (!spec.isPresent()) {
                throw new InvalidSettingsException(ERROR_MSG);
            }
            return spec;
        }

        private static DatabricksAccessTokenCredential resolveCredential(
            final DatabricksWorkspacePortObjectSpec spec) throws InvalidSettingsException {
            try {
                return spec.resolveCredential(DatabricksAccessTokenCredential.class);
            } catch (final NoSuchCredentialException e) {
                throw new InvalidSettingsException(ERROR_MSG, e);
            }
        }

        private static String normalizeWorkingDirectory(final String workingDirectory) {
            if (StringUtils.isBlank(workingDirectory)
                || !workingDirectory.startsWith(UnityFileSystem.PATH_SEPARATOR)) {
                return UnityFileSystem.PATH_SEPARATOR;
            }
            return workingDirectory;
        }

        @SuppressWarnings("resource")
        private static void testConnection(final UnityFSConnection connection) throws IOException {
            FSFileSystem<?> fileSystem = connection.getFileSystem();
            ((UnityFileSystem)fileSystem).testConnection();
        }
    }

    @Override
    public void validate() throws InvalidSettingsException {
        if (StringUtils.isAllBlank(m_workingDirectory)) {
            throw new InvalidSettingsException("Please specify a working directory.");
        } else if (!m_workingDirectory.startsWith(UnityFileSystem.PATH_SEPARATOR)) {
            throw new InvalidSettingsException("Working directory must be an absolute path that starts with \"/\"");
        }
    }
}
