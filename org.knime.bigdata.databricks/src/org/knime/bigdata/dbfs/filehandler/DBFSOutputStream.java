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
 */
package org.knime.bigdata.dbfs.filehandler;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;

/**
 * {@link OutputStream} to write files in 1MB blocks to DBFS.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class DBFSOutputStream extends OutputStream {
    private static final int BLOCK_SIZE = 1024 * 1024;

    private final String m_path;

    private final DBFSConnection m_connection;

    private boolean m_isOpen;

    private long m_handle;

    private final Encoder m_encoder;

    private final byte[] m_buffer;

    private int m_bufferOffset;

    DBFSOutputStream(final String path, final DBFSConnection connection, final boolean overwrite) throws IOException {
        m_path = path;
        m_connection = connection;
        m_isOpen = true;
        m_encoder = Base64.getEncoder();
        m_buffer = new byte[BLOCK_SIZE];
        m_bufferOffset = 0;

        try {
            m_handle = connection.createHandle(path, overwrite);
        } catch (Exception e) {
            throw new IOException("Failed to open handle for " + m_path, e);
        }
    }

    void submitBufferIfNecessary(final boolean flush) throws IOException {
        try {
            if (m_bufferOffset > 0 && (m_bufferOffset == m_buffer.length || flush)) {
                final String base64Data;

                if (m_bufferOffset == m_buffer.length) {
                    base64Data = m_encoder.encodeToString(m_buffer);
                } else {
                    base64Data = m_encoder.encodeToString(Arrays.copyOf(m_buffer, m_bufferOffset));
                }

                m_connection.addBlock(m_handle, base64Data);
                m_bufferOffset = 0;
            }
        } catch (Exception e) {
            throw new IOException("Failed to submit buffer while writing " + m_path, e);
        }
    }

    @Override
    public synchronized void write(final int b) throws IOException {
        if (m_isOpen) {
            submitBufferIfNecessary(false);
            m_buffer[m_bufferOffset++] = (byte)b;
        } else {
            throw new IOException("Stream already closed");
        }
    }

    @Override
    public void write(final byte[] src, final int off, final int len) throws IOException {

        if (!m_isOpen) {
            throw new IOException("Stream already closed");
        }

        int bytesLeft = len;
        int srcOffset = off;
        while (bytesLeft > 0) {
            submitBufferIfNecessary(false);
            int bytesToWrite = Math.min(bytesLeft, m_buffer.length - m_bufferOffset);
            System.arraycopy(src, srcOffset, m_buffer, m_bufferOffset, bytesToWrite);
            bytesLeft -= bytesToWrite;
            srcOffset += bytesToWrite;
            m_bufferOffset += bytesToWrite;
        }
    }

    @Override
    public void flush() throws IOException {
        if (m_isOpen) {
            submitBufferIfNecessary(true);
        } else {
            throw new IOException("Stream already closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (m_isOpen) {
            try {
                flush();
                m_connection.closeHandle(m_handle);

            } catch (Exception e) {
                throw new IOException("Failed to close handle while writing " + m_path, e);

            } finally {
                m_isOpen = false;
            }
        } else {
            throw new IOException("Stream already closed");
        }
    }
}
