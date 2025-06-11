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

import java.io.IOException;
import java.util.ArrayList;
import java.util.OptionalLong;

import org.apache.hadoop.fs.FileSystem;
import org.knime.bigdata.delta.types.DeltaTableColumn;
import org.knime.bigdata.delta.types.DeltaTableRandomAccessibleRow;
import org.knime.bigdata.delta.types.DeltaTableTypeHelper;
import org.knime.bigdata.delta.types.DeltaTableValue;
import org.knime.filehandling.core.node.table.reader.randomaccess.RandomAccessible;
import org.knime.filehandling.core.node.table.reader.read.Read;

import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;

/**
 * {@link Read} implementation that process scan file rows.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public final class DeltaTableRead implements Read<DeltaTableValue> {

    private final DeltaTableRandomAccessibleRow m_randomAccessibleRow;

    private final FileSystem m_fileSystem;

    private final DeltaTableRowIterator m_scanFileRowIterator;

    private final DeltaTableScanFileBatchIterator m_batchIterator;

    private final DeltaTableRowIterator m_rowIterator;

    private long m_rowsRead;

    /**
     * Default constructor.
     *
     * @param engine Delta engine to use
     * @param scanState Delta table scan state
     * @param scanFileRowIterator Iterator of scan files this read should process
     * @param physicalReadSchema schema to read
     */
    DeltaTableRead(final FileSystem fs, final Engine engine, final Row scanState,
        final DeltaTableRowIterator scanFileRowIterator, final StructType physicalReadSchema) {

        m_fileSystem = fs;
        m_scanFileRowIterator = scanFileRowIterator;
        m_batchIterator =
            new DeltaTableScanFileBatchIterator(engine, scanState, scanFileRowIterator, physicalReadSchema);
        m_rowIterator = new DeltaTableRowIterator(m_batchIterator);
        m_randomAccessibleRow = new DeltaTableRandomAccessibleRow(getColumns(physicalReadSchema));
        m_rowsRead = 0;
    }

    private static DeltaTableColumn[] getColumns(final StructType schema) {
        final var columns = new ArrayList<DeltaTableColumn>();
        final var fields = schema.fields();

        for (var ordinal = 0; ordinal < fields.size(); ordinal++) {
            columns.add(DeltaTableTypeHelper.getParquetColumn(fields.get(ordinal), ordinal, false));
        }

        return columns.toArray(new DeltaTableColumn[0]);
    }

    @Override
    public RandomAccessible<DeltaTableValue> next() throws IOException {
        if (m_rowIterator.hasNext()) {
            final var row = m_rowIterator.next();
            m_randomAccessibleRow.setRow(row);
            m_rowsRead++;
            return m_randomAccessibleRow;
        } else {
            return null;
        }
    }

    @Override
    public OptionalLong getMaxProgress() {
        return OptionalLong.empty(); // Default Delta Kernel does not expose the total row count
    }

    @Override
    public long getProgress() {
        return m_rowsRead;
    }

    @Override
    public boolean needsDecoration() {
        return false;
    }

    @Override
    public void close() throws IOException {
        Utils.closeCloseables(m_rowIterator, m_batchIterator, m_scanFileRowIterator);
        m_fileSystem.close();
    }

}
