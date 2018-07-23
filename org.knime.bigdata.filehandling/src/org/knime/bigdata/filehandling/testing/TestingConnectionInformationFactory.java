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
 *   Created on Jul 20, 2018 by bjoern
 */
package org.knime.bigdata.filehandling.testing;

import java.util.Map;

import org.knime.base.filehandling.remote.connectioninformation.node.AuthenticationMethod;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Provides factory methods to create {@link ConnectionInformation} objects for testing purposes.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @noreference This is testing code and its API is subject to change without notice.
 */
public class TestingConnectionInformationFactory {

    /**
     * Creates a {@link ConnectionInformation} with the given protocol and settings from the given map of flow
     * variables.
     *
     * @param protocol The remote file protocol to use, e.g. {@link HDFSRemoteFileHandler#HTTPFS_PROTOCOL}.
     * @param flowVariables A map of flow variables that provide the connection settings.
     * @return a {@link ConnectionInformation}.
     */
    public static ConnectionInformation create(final Protocol protocol, final Map<String, FlowVariable> flowVariables) {

        final ConnectionInformation remoteFsConnectionInfo = new ConnectionInformation();
        remoteFsConnectionInfo.setProtocol(protocol.getName());
        remoteFsConnectionInfo.setHost(TestflowVariable.getString(TestflowVariable.HOSTNAME, flowVariables));
        remoteFsConnectionInfo.setPort(protocol.getPort());

        final String authMethod = TestflowVariable.getString(TestflowVariable.HDFS_AUTH_METHOD, flowVariables);
        if (authMethod.equalsIgnoreCase(AuthenticationMethod.PASSWORD.toString())) {
            remoteFsConnectionInfo.setUser(TestflowVariable.getString(TestflowVariable.HDFS_USERNAME, flowVariables));
            remoteFsConnectionInfo.setPassword(""); // hdfs ignores passwords
        } else if (authMethod.equalsIgnoreCase(AuthenticationMethod.KERBEROS.toString())) {
            remoteFsConnectionInfo.setUseKerberos(true);
        }

        if (!HDFSRemoteFileHandler.isSupportedConnection(remoteFsConnectionInfo)) {
            throw new IllegalArgumentException("Unsupported protocol: " + protocol.getName());
        }

        return remoteFsConnectionInfo;
    }

}
