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
package org.knime.bigdata.filehandling.knox;

import java.net.URI;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileHandler;
import org.knime.core.node.util.CheckUtils;

/**
 * Web HDFS via KNOX implementation of the {@link RemoteFileHandler} interface.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class KnoxHDFSRemoteFileHandler implements RemoteFileHandler<KnoxHDFSConnection> {

    /**The {@link Protocol} of this {@link RemoteFileHandler}.*/
    public static final Protocol KNOXHDFS_PROTOCOL =
        new Protocol("knoxhdfs", 8443, true, false, false, false, true, true, true, true, false);

    @Override
    public Protocol[] getSupportedProtocols() {
        return new Protocol[] { KNOXHDFS_PROTOCOL };
    }

    /**
     * @param connectionInformation - Connection to check
     * @return <code>true</code> if this handler supports given connection.
     */
    public static boolean isSupportedConnection(final ConnectionInformation connectionInformation) {
        return KNOXHDFS_PROTOCOL.getName().equalsIgnoreCase(connectionInformation.getProtocol());
    }

    @Override
    public RemoteFile<KnoxHDFSConnection> createRemoteFile(final URI uri, final ConnectionInformation connectionInformation,
        final ConnectionMonitor<KnoxHDFSConnection> connectionMonitor) throws Exception {

        CheckUtils.checkNotNull(connectionInformation, "KNOX connection informations required.");
        CheckUtils.checkArgument(connectionInformation instanceof KnoxHDFSConnectionInformation,
            "Connection information to be expected of class %s but it is %s",
            KnoxHDFSConnectionInformation.class.getSimpleName(),
            connectionInformation == null ? "<null>" : connectionInformation.getClass().getSimpleName());

        return new KnoxHDFSRemoteFile(uri, (KnoxHDFSConnectionInformation)connectionInformation, connectionMonitor);
    }
}
