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
 * History 05.06.2018 (Mareike Hoeger): created
 */

package org.knime.bigdata.fileformats.parquet.reader;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.parquet.hadoop.ParquetReader;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.fileformats.node.reader.FileFormatRowIterator;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.BlobSupportDataRow;

class ParquetRowIterator extends FileFormatRowIterator {
    long m_index;
    private final ParquetReader<DataRow> m_reader;
    private DataRow m_nextRow;
    private final RemoteFile<Connection> m_tempInputFile;

    /**
     * @param index the index to start the row key
     * @param reader the reader to use
     * @param tempInputFile temporary input file that should be removed on close
     */
    public ParquetRowIterator(final long index, final ParquetReader<DataRow> reader, final RemoteFile<Connection> tempInputFile) {
        super();
        m_index = index;
        m_reader = reader;
        m_nextRow = internalNext();
        m_tempInputFile = tempInputFile;
    }

    public ParquetRowIterator(final long index, final ParquetReader<DataRow> reader) {
        this(index, reader, null);
    }


    @Override
    public void close() {
        try {
            m_reader.close();
        } catch (final IOException e) {
            throw new BigDataFileFormatException("Could not close reader", e);
        } finally {
            removeTempInputFile();
        }
    }

    private void removeTempInputFile() {
        if (m_tempInputFile != null) {
            try {
                m_tempInputFile.delete();
            } catch (Exception e) {
                throw new BigDataFileFormatException("Could not delete temporary file: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public long getIndex() {
        return m_index;
    }

    @Override
    public boolean hasNext() {
        return m_nextRow != null;
    }

    private DataRow internalNext() {
        try {
            return m_reader.read();
        } catch (final IOException e) {
            throw new BigDataFileFormatException(e);
        }
    }

    @Override
    public DataRow next() {

        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        DataRow currentRow = m_nextRow;
        m_nextRow = internalNext();
        currentRow = new BlobSupportDataRow(RowKey.createRowKey(m_index), currentRow);
        m_index++;
        return currentRow;
    }
}
