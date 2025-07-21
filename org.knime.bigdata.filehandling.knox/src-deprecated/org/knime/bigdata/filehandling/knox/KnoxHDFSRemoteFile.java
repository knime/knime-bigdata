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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.AccessDeniedException;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.filehandling.knox.rest.FileStatus;
import org.knime.bigdata.filehandling.knox.rest.FileStatus.Type;
import org.knime.core.node.ExecutionContext;

/**
 * Web HDFS via KNOX remote file implementation with file info cache.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@Deprecated
public class KnoxHDFSRemoteFile extends RemoteFile<KnoxHDFSConnection> {

    private boolean m_fileInfoLoaded = false;
    private FileStatus m_fileStatus;

    /**
     * Create a remote file.
     *
     * @param uri The URI pointing to the file
     * @param connectionInformation Connection information to the file
     * @param connectionMonitor Monitor for the connection
     * @deprecated
     */
    @Deprecated
    protected KnoxHDFSRemoteFile(final URI uri, final KnoxHDFSConnectionInformation connectionInformation,
        final ConnectionMonitor<KnoxHDFSConnection> connectionMonitor) {
        super(uri, connectionInformation, connectionMonitor);
    }

    /**
     * Create a remote file with cached {@link FileStatus}.
     *
     * @param fileStatus cached file info
     * @param uri The URI pointing to the file
     * @param connectionInformation Connection information to the file
     * @param connectionMonitor Monitor for the connection
     * @deprecated
     */
    @Deprecated
    protected KnoxHDFSRemoteFile(final FileStatus fileStatus, final URI uri,
            final ConnectionInformation connectionInformation,
            final ConnectionMonitor<KnoxHDFSConnection> connectionMonitor) {
        super(uri, connectionInformation, connectionMonitor);

        m_fileInfoLoaded = true;
        m_fileStatus = fileStatus;
    }

    private KnoxHDFSConnection getOpenedConnection() throws IOException {
        try {
            open();
            return getConnection();
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

    private FileStatus getFileStatus() throws IOException {
        if (!m_fileInfoLoaded) {
            try {
                m_fileStatus = getOpenedConnection().getFileStatus(getURI().getPath());
                m_fileInfoLoaded = true;
            } catch (AccessDeniedException e) {
                throw new RemoteFile.AccessControlException(e);
            } catch (FileNotFoundException e) { // NOSONAR
                m_fileInfoLoaded = true;
                m_fileStatus = null;
                throw new FileNotFoundException(getURI().getPath());
            }
        } else if (m_fileStatus == null) {
            throw new FileNotFoundException(getURI().getPath());
        }

        return m_fileStatus;
    }

    @Override
    protected boolean usesConnection() {
        return true;
    }

    @Override
    protected KnoxHDFSConnection createConnection() {
        return new KnoxHDFSConnection((KnoxHDFSConnectionInformation) getConnectionInformation());
    }

    @Override
    public String getType() {
        return KnoxHDFSRemoteFileHandler.KNOXHDFS_PROTOCOL.getName();
    }

    /**
     * Create a unique connection identifier using the basic schema and the path from the KNOX end point that includes
     * the KNOX topology/cluster name. This is required because a connection to a KNOX instance can use different
     * topologies with e.g. different authentication methods using the same username.
     *
     * @return unique connection identifier
     * @deprecated
     */
    @Deprecated
    @Override
    public String getIdentifier() {
        if (getConnectionInformation() == null) {
            throw new RuntimeException("KNOX connection informations input required.");
        } else {
            return ((KnoxHDFSConnectionInformation) getConnectionInformation()).getIdentifier();
        }
    }

    @Override
    public boolean exists() throws Exception {
        try {
            // do not cache the exists state
            invalidateCachedFileInfo();
            getFileStatus();
            return true;
        } catch (FileNotFoundException e) { // NOSONAR
            return false;
        }
    }

    @Override
    public boolean isDirectory() throws Exception {
        return getFileStatus().type == Type.DIRECTORY;
    }

    @Override
    public InputStream openInputStream() throws Exception {
        return getOpenedConnection().open(getURI().getPath());
    }

    @Override
    public OutputStream openOutputStream() throws Exception {
        invalidateCachedFileInfo();
        return getOpenedConnection().create(getURI().getPath(), true);
    }

    @Override
    public long getSize() throws Exception {
        return getFileStatus().length;
    }

    @Override
    public long lastModified() throws Exception {
        return getFileStatus().modificationTime;
    }

    @Override
    public void move(final RemoteFile<KnoxHDFSConnection> source, final ExecutionContext exec) throws Exception {
        invalidateCachedFileInfo();
        if (getIdentifier().equals(source.getIdentifier())) {
            getOpenedConnection().move(source.getURI().getPath(), getURI().getPath());
        } else {
            super.move(source, exec);
        }
    }

    @Override
    public boolean delete() throws Exception {
        invalidateCachedFileInfo();
        return getOpenedConnection().delete(getURI().getPath(), true);
    }

    @Override
    public KnoxHDFSRemoteFile[] listFiles() throws Exception {
        try {
            final FileStatus[] fileStatus = getOpenedConnection().listFiles(getURI().getPath());
            final KnoxHDFSRemoteFile[] files = new KnoxHDFSRemoteFile[fileStatus.length];
            for (int i = 0, length = fileStatus.length; i < length; i++) {
                final URI uri = getURI(getURI(), fileStatus[i]);
                files[i] = new KnoxHDFSRemoteFile(fileStatus[i], uri, getConnectionInformation(), getConnectionMonitor());
            }
            return files;
        } catch (AccessDeniedException e) {
            throw new RemoteFile.AccessControlException(e);
        }
    }

    private static URI getURI(final URI parent, final FileStatus child) {
        if (StringUtils.isBlank(child.pathSuffix)) {
            return parent;
        } else {
            final String parentPath = parent.getPath();
            final StringBuilder sb = new StringBuilder(parentPath);
            if (!parentPath.endsWith("/")) { sb.append("/"); }
            sb.append(child.pathSuffix);
            if (child.type == Type.DIRECTORY) { sb.append("/"); }
            return parent.resolve(sb.toString());
        }
    }

    @Override
    public boolean mkDir() throws Exception {
        invalidateCachedFileInfo();
        return getOpenedConnection().mkdirs(getURI().getPath());
    }

    private void invalidateCachedFileInfo() {
        m_fileInfoLoaded = false;
        m_fileStatus = null;
    }
}
