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
 *   2025-05-21 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.delta.nodes.reader.framework;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Iterator that consumes scan files and returns physical data batches.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("resource")
public class DeltaTableScanFileBatchIterator implements CloseableIterator<FilteredColumnarBatch> {

    private final Engine m_engine;

    private final Row m_scanState;

    private final StructType m_physicalReadSchema;

    private final DeltaTableRowIterator m_scanFilesRowIterator;

    private CloseableIterator<FilteredColumnarBatch> m_currentBatchIterator;

    DeltaTableScanFileBatchIterator(final Engine engine, final Row scanState,
        final DeltaTableRowIterator scanFilesRowIterator, final StructType physicalReadSchema) {

        m_engine = engine;
        m_scanState = scanState;
        m_physicalReadSchema = physicalReadSchema;
        m_scanFilesRowIterator = scanFilesRowIterator;
    }

    @Override
    public boolean hasNext() {
        if ((m_currentBatchIterator != null) && m_currentBatchIterator.hasNext()) {
            return true;
        }

        // close current rows iterator
        Utils.closeCloseables(m_currentBatchIterator);
        m_currentBatchIterator = null;

        // load next iterator
        if (m_scanFilesRowIterator.hasNext()) {
            m_currentBatchIterator = nextBatchIterator(m_scanFilesRowIterator.next());
            return m_currentBatchIterator.hasNext();
        }

        // no batches left
        return false;
    }

    @Override
    public FilteredColumnarBatch next() {
        if (m_currentBatchIterator != null) {
            return m_currentBatchIterator.next();
        } else {
            throw new NoSuchElementException();
        }
    }

    private CloseableIterator<FilteredColumnarBatch> nextBatchIterator(final Row scanFileRow) {
        try {
            final var fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
            final var physicalDataIter = m_engine.getParquetHandler().readParquetFiles( //
                singletonCloseableIterator(fileStatus), //
                m_physicalReadSchema, //
                Optional.empty()); // value filter
            return Scan.transformPhysicalData(m_engine, m_scanState, scanFileRow, physicalDataIter);
        } catch (final IOException e) {
            throw new KernelException(e);
        }
    }

    @Override
    public void close() throws IOException {
        Utils.closeCloseables(m_currentBatchIterator);
    }

}
