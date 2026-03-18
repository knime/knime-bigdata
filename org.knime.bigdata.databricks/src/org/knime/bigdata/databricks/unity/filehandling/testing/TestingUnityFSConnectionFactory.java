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
 *   2026-03-16 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.databricks.unity.filehandling.testing;

import java.time.Duration;
import java.util.Map;

import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.credential.TestingDatabricksAccessTokenCredentialFactory;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSConnection;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSConnectionConfig;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSDescriptorProvider;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Provides factory methods to create {@link UnityFSConnection} instance for testing purposes.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 * @noreference This is testing code and its API is subject to change without notice.
 */
public final class TestingUnityFSConnectionFactory {

    private TestingUnityFSConnectionFactory() {
    }

    /**
     * Creates a {@link FileSystemPortObjectSpec} from the given map of flow variables.
     *
     * @param fsId file system connection identifier
     * @param flowVariables A map of flow variables that provide the Spark context settings.
     * @return a {@link FileSystemPortObjectSpec} of the {@link UnityFSConnection}
     * @throws InvalidSettingsException on invalid or missing flow variables
     */
    public static FileSystemPortObjectSpec createFileSystemPortSpec(final String fsId,
        final Map<String, FlowVariable> flowVariables) throws InvalidSettingsException {

        final UnityFSConnectionConfig config = fromFlowVariables(flowVariables);
        return createSpec(fsId, config);
    }

    private static FileSystemPortObjectSpec createSpec(final String fsId, final UnityFSConnectionConfig config) {
        return new FileSystemPortObjectSpec(UnityFSDescriptorProvider.FS_TYPE.getTypeId(), //
            fsId, //
            config.createFSLocationSpec());
    }

    /**
     * Creates a {@link UnityFSConnection} from the given map of flow variables.
     *
     * @param flowVariables A map of flow variables that provide the Spark context settings.
     * @return a {@link UnityFSConnection} instance
     * @throws InvalidSettingsException on invalid or missing flow variables
     */
    public static FSConnection createFSConnection(final Map<String, FlowVariable> flowVariables)
        throws InvalidSettingsException {

        final UnityFSConnectionConfig config = fromFlowVariables(flowVariables);
        return new UnityFSConnection(config);
    }

    private static UnityFSConnectionConfig fromFlowVariables(final Map<String, FlowVariable> flowVariables)
        throws InvalidSettingsException {

        final DatabricksAccessTokenCredential credentials =
            TestingDatabricksAccessTokenCredentialFactory.fromFlowVariables(flowVariables);

        if (!flowVariables.containsKey(TestflowVariable.SPARK_DATABRICKS_UNITY_VOLUME.getName())) {
            throw new InvalidSettingsException("Databricks Unity volume flow variable is required.");
        }

        if (!flowVariables.containsKey(TestflowVariable.SPARK_DATABRICKS_CONNECTIONTIMEOUT.getName())) {
            throw new InvalidSettingsException("Databricks connection timeout flow variable is required.");
        }

        if (!flowVariables.containsKey(TestflowVariable.SPARK_DATABRICKS_RECEIVETIMEOUT.getName())) {
            throw new InvalidSettingsException("Databricks receive timeout flow variable is required.");
        }

        final String workingDirectory =
            TestflowVariable.getString(TestflowVariable.SPARK_DATABRICKS_UNITY_VOLUME, flowVariables);
        final int connectionTimeout =
            TestflowVariable.getInt(TestflowVariable.SPARK_DATABRICKS_CONNECTIONTIMEOUT, flowVariables);
        final int receiveTimeout =
            TestflowVariable.getInt(TestflowVariable.SPARK_DATABRICKS_RECEIVETIMEOUT, flowVariables);

        return UnityFSConnectionConfig.builder() //
            .withCredential(credentials) //
            .withWorkingDirectory(workingDirectory) //
            .withConnectionTimeout(Duration.ofSeconds(connectionTimeout)) //
            .withReadTimeout(Duration.ofSeconds(receiveTimeout)) //
            .build();
    }

}
