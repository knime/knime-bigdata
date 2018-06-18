/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 *
 * History 24 Apr 2018 (Marc Bux, KNIME AG, Zurich, Switzerland): created
 */
package org.knime.bigdata.fileformats.parquet.reader;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.knime.bigdata.fileformats.parquet.ParquetTableStoreFormat;
import org.knime.bigdata.fileformats.parquet.type.ParquetType;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.storage.AbstractTableStoreReader;
import org.knime.core.node.util.CheckUtils;

/**
 * A class for reading Parquet files on the disk and converting them to KNIME
 * tables.
 *
 * @author Mareike Hoeger, KNIME AG, Zurich, Switzerland
 * @author Marc Bux, KNIME AG, Zurich, Switzerland
 */
public final class ParquetTableStoreReader extends AbstractTableStoreReader {
    private final ParquetType[] m_columnTypes;

    private final boolean m_readRowKey;

    private final File m_file;

    /**
     * Constructs a reader reading binary Parquet files on the local disk and
     * converting them to KNIME table. {@link ParquetWriter}.
     *
     * @param file the local file from which to read
     * @param spec the specification of the KNIME table to derive from the
     *        Parquet file
     * @param readRowKey a flag that determines whether to obtain the row keys
     *        from the Parquet file
     */
    public ParquetTableStoreReader(final File file, final DataTableSpec spec, final boolean readRowKey) {
        m_columnTypes = CheckUtils.checkArgumentNotNull(ParquetTableStoreFormat.parquetTypesFromSpec(spec));
        m_readRowKey = readRowKey;
        m_file = CheckUtils.checkArgumentNotNull(file);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableStoreCloseableRowIterator iterator() throws IOException {
        return new ParquetRowIterator(m_file, m_columnTypes, m_readRowKey);
    }

    /**
     * An iterator for iterating over the {@link DataRow} instances obtained
     * from a Parquet file.
     */
    static final class ParquetRowIterator extends TableStoreCloseableRowIterator {
        private final ParquetReader<DataRow> m_reader;

        private DataRow m_nextRow;

        ParquetRowIterator(final File file, final ParquetType[] columnTypes, final boolean readRowKey)
                throws IOException {
            m_reader = ParquetReader//
                    .builder(new DataRowReadSupport(columnTypes, readRowKey), new Path(file.getAbsolutePath()))//
                    .build();

            m_nextRow = internalNext();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return m_nextRow != null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final DataRow currentRow = m_nextRow;
            m_nextRow = internalNext();
            return currentRow;
        }

        private DataRow internalNext() {
            try {
                return m_reader.read();
            } catch (final IOException e) {
                throw new BigDataFileFormatException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean performClose() throws IOException {
            m_reader.close();
            return true;
        }
    }
}
