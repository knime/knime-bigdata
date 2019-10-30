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
import java.io.InputStream;
import java.util.Base64;
import java.util.Base64.Decoder;

import org.knime.bigdata.databricks.rest.dbfs.FileBlock;

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
            final FileBlock fileBlock = m_connection.readBlock(m_path, m_nextOffset, BLOCK_SIZE);
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
