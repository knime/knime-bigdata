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
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.knime.bigdata.fileformats.filehandling.reader.BigDataCell;
import org.knime.bigdata.fileformats.filehandling.reader.orc.cell.OrcCell;
import org.knime.bigdata.fileformats.filehandling.reader.orc.cell.OrcCellFactory;
import org.knime.filehandling.core.node.table.reader.randomaccess.RandomAccessible;
import org.knime.filehandling.core.node.table.reader.read.Read;

/**
 * A {@link Read} of an ORC file.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class OrcRead implements Read<BigDataCell> {

    private final long m_rowCount;

    private final RecordReader m_rows;

    private final Path m_path;

    private final VectorizedRowBatch m_batch;

    private final OrcRandomAccessible m_randomAccessible;

    private final OrcCell[] m_cells;

    private int m_batchIndex = -1;

    private long m_rowCounter = 0;

    OrcRead(final Reader reader, final Path path) throws IOException {
        m_rowCount = reader.getNumberOfRows();
        m_rows = reader.rows();
        m_batch = reader.getSchema().createRowBatch();
        m_path = path;
        m_randomAccessible = new OrcRandomAccessible();
        m_cells = reader.getSchema().getChildren().stream().map(OrcCellFactory::create).toArray(OrcCell[]::new);
    }

    @Override
    public RandomAccessible<BigDataCell> next() throws IOException {
        m_rowCounter++;

        if (m_rowCounter == 1 || m_batchIndex >= m_batch.size) {
            if (m_rows.nextBatch(m_batch)) {
                m_batchIndex = 0;
                for (int i = 0; i < m_cells.length; i++) {
                    m_cells[i].setColVector(m_batch.cols[i]);
                }
            } else {
                return null;
            }
        }
        for (OrcCell cell : m_cells) {
            cell.setIndexInColumn(m_batchIndex);
        }
        m_batchIndex++;

        return m_randomAccessible;
    }

    /**
     * {@link RandomAccessible} of {@link OrcCell OrcCells}.
     * It always returns the same cells which internally read from a {@link ColumnVector}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    private class OrcRandomAccessible implements RandomAccessible<BigDataCell> {

        @Override
        public int size() {
            return m_cells.length;
        }

        @Override
        public OrcCell get(final int idx) {
            final OrcCell cell = m_cells[idx];
            return cell.isNull() ? null : cell;
        }

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
    public Optional<Path> getPath() {
        return Optional.of(m_path);
    }

    @Override
    public void close() throws IOException {
        m_rows.close();
    }

}
