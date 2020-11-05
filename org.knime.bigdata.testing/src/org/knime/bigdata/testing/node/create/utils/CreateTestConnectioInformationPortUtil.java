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
 */
package org.knime.bigdata.testing.node.create.utils;

import java.net.URI;
import java.util.Map;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.bigdata.dbfs.testing.TestingDBFSConnectionInformationFactory;
import org.knime.bigdata.filehandling.local.HDFSLocalConnectionInformation;
import org.knime.bigdata.filehandling.testing.TestingConnectionInformationFactory;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Utility to create {@link ConnectionInformation} based file system output ports.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class CreateTestConnectioInformationPortUtil implements CreateTestPortUtil {

    private ConnectionInformation m_fsConnectionInfo;

    /**
     * Output test port type of this utility.
     */
    public static final PortType PORT_TYPE = ConnectionInformationPortObject.TYPE;

    @Override
    public PortObjectSpec configure(final SparkContextIDScheme sparkScheme, final Map<String, FlowVariable> flowVars)
        throws InvalidSettingsException {

        m_fsConnectionInfo = createConnectionInformation(flowVars, sparkScheme);
        return new ConnectionInformationPortObjectSpec(m_fsConnectionInfo);
    }

    @Override
    public PortObject execute(final SparkContextIDScheme sparkScheme, final Map<String, FlowVariable> flowVars,
        final ExecutionContext exec, final CredentialsProvider credentialsProvider) throws Exception {

        testRemoteFsConnection(m_fsConnectionInfo);
        return new ConnectionInformationPortObject(new ConnectionInformationPortObjectSpec(m_fsConnectionInfo));
    }

    /**
     * Create a {@link ConnectionInformation} based on a {@link SparkContextIDScheme} using flow variables.
     *
     * @param flowVars test workflow variables
     * @param sparkScheme spark scheme to match
     * @return file system connection information
     * @throws InvalidSettingsException
     */
    private static ConnectionInformation createConnectionInformation(final Map<String, FlowVariable> flowVars,
        final SparkContextIDScheme sparkScheme) throws InvalidSettingsException {

        switch (sparkScheme) {
            case SPARK_LOCAL:
                return HDFSLocalConnectionInformation.getInstance();
            case SPARK_JOBSERVER:
            case SPARK_LIVY:
                return TestingConnectionInformationFactory.create(HDFSRemoteFileHandler.HTTPFS_PROTOCOL, flowVars);
            case SPARK_DATABRICKS:
                return TestingDBFSConnectionInformationFactory.create(flowVars);
            default:
                throw new InvalidSettingsException(
                    "Spark context ID scheme not supported: " + sparkScheme);
        }
    }

    /**
     * Test if the root of the given file system exists to check if the connection works.
     *
     * @param connInfo file system connection to check
     * @throws Exception
     */
    private static void testRemoteFsConnection(final ConnectionInformation connInfo) throws Exception {
        final ConnectionMonitor<?> monitor = new ConnectionMonitor<>();

        try {
            final URI uri = connInfo.toURI().resolve("/");
            final RemoteFile<? extends Connection> file = RemoteFileFactory.createRemoteFile(uri, connInfo, monitor);
            if (file != null) {
                //perform a simple operation to check that the connection really exists and is valid
                file.exists();
            }
        } finally {
            monitor.closeAll();
        }
    }
}
