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
 *
 * History
 *   Apr 8, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.orc;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.knime.core.columnar.ColumnarSchema;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.SequentialBatchReadable;
import org.knime.core.columnar.batch.SequentialBatchReader;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.filter.ColumnSelection;

/**
 * {@link SequentialBatchReadable} based on an ORC {@link Reader}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class OrcSequentialBatchReadable implements SequentialBatchReadable {

    private final Reader m_reader;

    private final ColumnarSchema m_schema;

    OrcSequentialBatchReadable(final Reader reader) {
        m_reader = reader;
        m_schema = OrcTypeUtils.convert(reader.getSchema());
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    @Override
    public void close() throws IOException {
        // nothing to close
    }

    @Override
    public SequentialBatchReader createReader(final ColumnSelection selection) {
        try {
            return new OrcBatchReader(selection);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private final class OrcBatchReader implements SequentialBatchReader {

        private final RecordReader m_rows;

        private final VectorizedRowBatch m_batch;

        private final ColumnSelection m_columnSelection;

        OrcBatchReader(final ColumnSelection columnSelection) throws IOException {
            // TODO push column selection down
            m_rows = m_reader.rows();
            m_batch = m_reader.getSchema().createRowBatch();
            m_columnSelection = columnSelection;
        }

        @Override
        public void close() throws IOException {
            m_rows.close();
        }

        @Override
        public ReadBatch readRetained() throws IOException {
            m_rows.nextBatch(m_batch);
            return m_columnSelection.createBatch(this::getColumn);
        }

        private NullableReadData getColumn(final int index) {
            return OrcTypeUtils.convert(m_batch.cols[index], m_batch.size);
        }

    }

}
