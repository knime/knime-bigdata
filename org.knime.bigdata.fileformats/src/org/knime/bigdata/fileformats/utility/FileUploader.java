/*
 * ------------------------------------------------------------------------
 * Copyright by KNIME AG, Zurich, Switzerland Website: http://www.knime.com;
 * Email: contact@knime.com
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, Version 3, as published by the
 * Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, see <http://www.gnu.org/licenses>.
 *
 * Additional permission under GNU GPL version 3 section 7:
 *
 * KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs. Hence,
 * KNIME and ECLIPSE are both independent programs and are not derived from each
 * other. Should, however, the interpretation of the GNU GPL Version 3
 * ("License") under any applicable laws result in KNIME and ECLIPSE being a
 * combined program, KNIME AG herewith grants you the additional permission to
 * use and propagate KNIME together with ECLIPSE with only the license terms in
 * place for ECLIPSE applying to ECLIPSE and the GNU GPL Version 3 applying for
 * KNIME, provided the license terms of ECLIPSE themselves allow for the
 * respective use and propagation of ECLIPSE together with KNIME.
 *
 * Additional permission relating to nodes for KNIME that extend the Node
 * Extension (and in particular that are based on subclasses of NodeModel,
 * NodeDialog, and NodeView) and that only interoperate with KNIME through
 * standard APIs ("Nodes"): Nodes are deemed to be separate and independent
 * programs and to not be covered works. Notwithstanding anything to the
 * contrary in the License, the License does not apply to Nodes, you are not
 * required to license Nodes under the License, and you are granted a license to
 * prepare and propagate Nodes, in each case even if such Nodes are propagated
 * with or for interoperation with KNIME. The owner of a Node may freely choose
 * the license terms applicable to such Node, including when such Node is
 * propagated with or for interoperation with KNIME.
 * -------------------------------------------------------------------
 *
 * History 28.05.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.fileformats.utility;

import java.net.URI;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;

/**
 * Upload for temporary files from a queue to a remote destination.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class FileUploader implements Callable<String> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FileUploader.class);

    private final ArrayBlockingQueue<RemoteFile<Connection>> m_tempFileQueue;

    private final ExecutionContext m_exec;

    private boolean m_writeFinished = false;

    final RemoteFile<Connection> m_targetDir;

    final String m_filePrefix;

    /**
     * Upload for temporary files to a remote destination. It creates a
     * directory with the name given in {@code target} and stores the files with
     * the same name and a file count suffix in this directory.
     *
     * @param numberOfChunks the number of chunks allowed
     * @param target the target file
     * @param exec the execution context
     * @throws Exception if the directory or files can not be created.
     */
    public FileUploader(int numberOfChunks, RemoteFile<Connection> target, ExecutionContext exec) throws Exception {
        m_tempFileQueue = new ArrayBlockingQueue<>(numberOfChunks);
        m_exec = exec;
        final URI targetURI = new URI(String.format("%s", target.getURI().toString()));
        m_targetDir = RemoteFileFactory.createRemoteFile(targetURI, target.getConnectionInformation(),
                target.getConnectionMonitor());
        m_targetDir.mkDir();
        m_filePrefix = target.getName();
    }

    /**
     * Sets the writeFinished flag.
     *
     * @param writeFinished boolean for the flag
     */
    public void setWriteFinished(boolean writeFinished) {
        m_writeFinished = writeFinished;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String call() throws Exception {
        int fileCount = 0;
        while (true) {
            try {
                final RemoteFile<Connection> tempFile = m_tempFileQueue.take();
                final URI targetURI = new URI(
                        String.format("%s%s_%05d", m_targetDir.getURI().toString(), m_filePrefix, fileCount));
                final RemoteFile<Connection> targetFile = RemoteFileFactory.createRemoteFile(targetURI,
                        m_targetDir.getConnectionInformation(), m_targetDir.getConnectionMonitor());
                m_exec.setProgress(String.format("Writing remote File %s", targetURI.toString()));
                targetFile.write(tempFile, m_exec);
                fileCount++;
                tempFile.delete();
                if (m_writeFinished && m_tempFileQueue.isEmpty()) {
                    break;
                }
            } catch (final Exception e) {
                LOGGER.info("Upload error.", e);
                throw new BigDataFileFormatException(e.getMessage());
            }
        }
        return String.format("Uploaded %d files to %s", fileCount, m_targetDir);
    }

    /**
     * Adds a file to the upload Queue. This method waits until space is
     * available in the queue.
     *
     * @param file the file to add.
     * @throws InterruptedException if interrupted while waiting
     */
    public void addFile(final RemoteFile<Connection> file) throws InterruptedException {
        m_tempFileQueue.put(file);
    }
}
