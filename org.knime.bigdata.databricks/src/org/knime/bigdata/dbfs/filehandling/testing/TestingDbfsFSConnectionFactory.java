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
 *   Sep 23, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.dbfs.filehandling.testing;

import java.io.IOException;
import java.util.Map;

import org.knime.bigdata.dbfs.filehandler.DBFSConnection;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnection;
import org.knime.bigdata.spark.core.databricks.node.create.DatabricksSparkContextCreatorNodeSettings2;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Provides factory methods to create {@link DbfsFSConnection} instances for testing purposes.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @noreference This is testing code and its API is subject to change without notice.
 */
public final class TestingDbfsFSConnectionFactory {

    private TestingDbfsFSConnectionFactory() {
    }

    /**
     * Creates a {@link FileSystemPortObjectSpec} from the given map of flow variables.
     *
     * @param fsId file system connection identifier
     * @param flowVariables A map of flow variables that provide the Spark context settings.
     * @return a {@link FileSystemPortObjectSpec} of the {@link DBFSConnection}
     * @throws InvalidSettingsException on invalid or missing flow variables
     */
    public static FileSystemPortObjectSpec createFileSystemPortSpec(final String fsId,
        final Map<String, FlowVariable> flowVariables) throws InvalidSettingsException {

        return DatabricksSparkContextCreatorNodeSettings2 //
            .fromFlowVariables(flowVariables) //
            .createFileSystemSpec(fsId);
    }

    /**
     * Creates a {@link DbfsFSConnection} from the given map of flow variables.
     *
     * @param flowVariables A map of flow variables that provide the Spark context settings.
     * @return a {@link DbfsFSConnection} instance
     * @throws InvalidSettingsException on invalid or missing flow variables
     * @throws IOException on connection initialization failures
     */
    public static FSConnection createFSConnection(final Map<String, FlowVariable> flowVariables)
        throws InvalidSettingsException, IOException {

        return DatabricksSparkContextCreatorNodeSettings2 //
            .fromFlowVariables(flowVariables) //
            .createDatabricksFSConnection(null);
    }
}
