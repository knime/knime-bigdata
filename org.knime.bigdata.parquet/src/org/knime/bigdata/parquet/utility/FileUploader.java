package org.knime.bigdata.parquet.utility;

import java.net.URI;
import java.util.concurrent.ArrayBlockingQueue;

import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.bigdata.parquet.node.writer.ParquetWriteException;
import org.knime.core.node.ExecutionContext;

/**
 * Upload for temporary files from a queue to a remote destination.
 */
public class FileUploader implements Runnable {

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
     * @param fileQueue the blocking queue containing the temporary files
     * @param target the target file
     * @param exec the execution context
     * @throws Exception if the directory or files can not be created.
     */
    public FileUploader(ArrayBlockingQueue<RemoteFile<Connection>> fileQueue, RemoteFile<Connection> target,
            ExecutionContext exec) throws Exception {
        m_tempFileQueue = fileQueue;
        m_exec = exec;
        final URI targetURI = new URI(String.format("%s", target.getURI().toString()));
        m_targetDir = RemoteFileFactory.createRemoteFile(targetURI, target.getConnectionInformation(),
                target.getConnectionMonitor());
        m_targetDir.mkDir();
        m_filePrefix = target.getName();
    }

    @Override
    public void run() {
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
                throw new ParquetWriteException(e.getMessage());
            }
        }
    }

    /**
     * Sets the writeFinished flag.
     *
     * @param writeFinished boolean for the flag
     */
    public void setWriteFinished(boolean writeFinished) {
        m_writeFinished = writeFinished;
    }
}
