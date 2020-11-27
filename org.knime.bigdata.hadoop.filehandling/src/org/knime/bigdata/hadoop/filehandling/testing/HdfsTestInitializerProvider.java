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
package org.knime.bigdata.hadoop.filehandling.testing;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsFSConnection;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsFileSystem;
import org.knime.bigdata.hadoop.filehandling.node.HdfsAuthenticationSettings.AuthType;
import org.knime.bigdata.hadoop.filehandling.node.HdfsConnectorNodeSettings;
import org.knime.bigdata.hadoop.filehandling.node.HdfsProtocol;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.testing.DefaultFSTestInitializerProvider;

/**
 * HDFS test initializer provider.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class HdfsTestInitializerProvider extends DefaultFSTestInitializerProvider {

    @SuppressWarnings("resource")
    @Override
    public HdfsTestInitializer setup(final Map<String, String> configuration) throws IOException {
        validateConfiguration(configuration);

        final String workingDirPrefix = configuration.get("workingDirPrefix");
        final String workingDir = generateRandomizedWorkingDir(workingDirPrefix, HdfsFileSystem.PATH_SEPARATOR);

        final HdfsProtocol protocol = HdfsProtocol.valueOf(configuration.get("protocol").toUpperCase());
        int port = configuration.containsKey("port") ? Integer.parseInt(configuration.get("port")) : protocol.getDefaultPort();

        final HdfsConnectorNodeSettings settings = new HdfsConnectorNodeSettings( //
            protocol, //
            configuration.get("host"), //
            true, // custom port
            port, //
            AuthType.valueOf(configuration.get("auth").toUpperCase()), //
            configuration.get("user"), //
            workingDir);

        return new HdfsTestInitializer(new HdfsFSConnection(settings));
    }

    private static void validateConfiguration(final Map<String, String> configuration) {
        checkArgumentNotBlank(configuration.get("protocol"), "protocol must be specified (one of HDFS, WEBHDFS, WEBHDFS_SSL, HTTPFS, HTTPFS_SSL)");
        try {
            HdfsProtocol.valueOf(configuration.get("protocol").toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown protocol: " + configuration.get("protocol"));
        }

        checkArgumentNotBlank(configuration.get("host"), "host must be specified");

        if (configuration.containsKey("port")) {
            checkArgumentNotBlank(configuration.get("port"), "port is optional, but must not be blank if set");
        }

        checkArgumentNotBlank(configuration.get("workingDirPrefix"), "Working directory prefix must be specified.");

        checkArgumentNotBlank(configuration.get("auth"), "Auth type must be specified.");
        final AuthType authType = AuthType.valueOf(configuration.get("auth").toUpperCase());
        if (authType == AuthType.SIMPLE) {
            checkArgumentNotBlank(configuration.get("user"), "User must be specified.");
        } else if (authType != AuthType.KERBEROS) { // NOSONAR
            throw new IllegalArgumentException("Unknown authentication type.");
        }
    }

    private static void checkArgumentNotBlank(final String arg, final String message) {
        if (StringUtils.isBlank(arg)) {
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public String getFSType() {
        return HdfsFileSystem.FS_TYPE;
    }

    @Override
    public FSLocationSpec createFSLocationSpec(final Map<String, String> configuration) {
        validateConfiguration(configuration);
        final URI fsURI = URI.create(configuration.get("uri"));
        return HdfsFileSystem.createFSLocationSpec(fsURI.getHost());
    }
}
