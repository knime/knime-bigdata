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
import java.io.InputStream;
import java.util.Base64;
import java.util.Base64.Decoder;

import org.knime.bigdata.databricks.rest.dbfs.FileBlockResponse;

/**
 * {@link InputStream} to read files in 1MB blocks from DBFS.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class DBFSInputStream extends InputStream {
    private static final int BLOCK_SIZE = 1024 * 1024;

    private final String m_path;

    private final DBFSConnection m_connection;

    private long m_nextOffset;

    private final Decoder m_decoder;

    private final byte[] m_buffer;

    private int m_bufferOffset;

    private int m_bufferLength;

    private boolean m_lastBlock;

    DBFSInputStream(final String path, final DBFSConnection connection) throws IOException {
        m_path = path;
        m_connection = connection;
        m_nextOffset = 0;
        m_decoder = Base64.getDecoder();
        m_buffer = new byte[BLOCK_SIZE];
        m_bufferOffset = 0;
        m_bufferLength = 0;
        readNextBlockIfNecessary();
    }

    private void readNextBlockIfNecessary() throws IOException {
        if (!m_lastBlock && m_bufferOffset == m_bufferLength) {
            final FileBlockResponse fileBlock = m_connection.readBlock(m_path, m_nextOffset, BLOCK_SIZE);
            m_lastBlock = fileBlock.bytes_read < BLOCK_SIZE;
            m_bufferOffset = m_bufferLength = 0;

            if (fileBlock.bytes_read > 0) {
                m_nextOffset += fileBlock.bytes_read;
                m_bufferLength = m_decoder.decode(fileBlock.data.getBytes(), m_buffer);
            }
        }
    }

    @Override
    public synchronized int read() throws IOException {
        readNextBlockIfNecessary();

        if (m_lastBlock && m_bufferOffset == m_bufferLength) {
            return -1;
        } else {
            // return byte as int between 0 and 255
            return m_buffer[m_bufferOffset++] & 0xff;
        }
    }

    @Override
    public int read(final byte[] dest, final int off, final int len) throws IOException {

        readNextBlockIfNecessary();

        if (m_lastBlock && m_bufferOffset == m_bufferLength) {
            return -1;
        } else {
            final int bytesToRead = Math.min(len, m_bufferLength - m_bufferOffset);
            System.arraycopy(m_buffer, m_bufferOffset, dest, off, bytesToRead);
            m_bufferOffset += bytesToRead;
            return bytesToRead;
        }
    }
}
