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
 *   Nov 6, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.orc;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.bigdata.fileformats.filehandling.reader.orc.cell.OrcCell;
import org.knime.bigdata.fileformats.filehandling.reader.orc.cell.OrcCellFactory;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.node.table.reader.randomaccess.RandomAccessible;
import org.knime.filehandling.core.node.table.reader.read.Read;

/**
 * A {@link Read} of an ORC file.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class OrcRead implements Read<FSPath, BigDataCell> {

    private final long m_rowCount;

    private final RecordReader m_rows;

    private final FSPath m_path;

    private final VectorizedRowBatch m_batch;

    private final TypeDescription m_schema;

    private int m_batchIndex = 0;

    private long m_rowCounter = 0;


    OrcRead(final Reader reader, final FSPath path) throws IOException {
        m_rowCount = reader.getNumberOfRows();
        m_rows = reader.rows();
        m_batch = reader.getSchema().createRowBatch();
        m_path = path;
        m_schema = reader.getSchema();
    }

    @Override
    public RandomAccessible<BigDataCell> next() throws IOException {
        m_rowCounter++;
        final OrcCell[] cells = createCells();
        if (m_rowCounter == 1 || m_batchIndex >= m_batch.size) {
            if (m_rows.nextBatch(m_batch)) {
                m_batchIndex = 0;
            } else {
                return null;
            }
        }
        for (int i = 0; i < cells.length; i++) {
            cells[i].setColVector(m_batch.cols[i]);
            cells[i].setIndexInColumn(m_batchIndex);
        }
        for (OrcCell cell : cells) {
            cell.setIndexInColumn(m_batchIndex);
        }
        m_batchIndex++;

        return new OrcRandomAccessible(cells);
    }

    private OrcCell[] createCells() {
        return m_schema.getChildren().stream().map(OrcCellFactory::create).toArray(OrcCell[]::new);
    }

    @Override
    public OptionalLong getMaxProgress() {
        return OptionalLong.of(m_rowCount);
    }

    @Override
    public long getProgress() {
        return m_rowCounter;
    }

    @Override
    public Optional<FSPath> getItem() {
        return Optional.of(m_path);
    }

    @Override
    public void close() throws IOException {
        m_rows.close();
    }

    private static final class OrcRandomAccessible implements RandomAccessible<BigDataCell> {

        private final OrcCell[] m_cells;

        OrcRandomAccessible(final OrcCell[] cells) {
            m_cells = cells;
        }

        @Override
        public int size() {
            return m_cells.length;
        }

        @Override
        public BigDataCell get(final int idx) {
            OrcCell cell = m_cells[idx];
            return cell.isNull() ? null : cell;
        }

    }

}
