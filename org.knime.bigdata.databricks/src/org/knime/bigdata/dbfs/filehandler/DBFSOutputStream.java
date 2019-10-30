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
