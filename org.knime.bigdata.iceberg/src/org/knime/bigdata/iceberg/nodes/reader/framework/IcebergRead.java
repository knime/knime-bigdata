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
package org.knime.bigdata.iceberg.nodes.reader.framework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.OptionalLong;

import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types.StructType;
import org.knime.bigdata.iceberg.types.IcebergColumn;
import org.knime.bigdata.iceberg.types.IcebergRandomAccessibleRow;
import org.knime.bigdata.iceberg.types.IcebergTypeHelper;
import org.knime.bigdata.iceberg.types.IcebergValue;
import org.knime.filehandling.core.node.table.reader.randomaccess.RandomAccessible;
import org.knime.filehandling.core.node.table.reader.read.Read;

/**
 * {@link Read} implementation that process scan file rows.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public final class IcebergRead implements Read<IcebergValue> {

    private final IcebergRandomAccessibleRow m_randomAccessibleRow;

    private final FileSystem m_fileSystem;

    private final CloseableIterable<Record> m_recordsIterable;

    private final Iterator<Record> m_recordsIterator;

    private long m_rowsRead;

    /**
     * Default constructor.
     *
     * @param engine Delta engine to use
     * @param scanState Delta table scan state
     * @param scanFileRowIterator Iterator of scan files this read should process
     * @param physicalReadSchema schema to read
     */
    IcebergRead(final FileSystem fs, final CloseableIterable<Record> recordsIterable,
        final StructType physicalReadSchema) {

        m_fileSystem = fs;
        m_recordsIterable = recordsIterable;
        m_recordsIterator = recordsIterable.iterator();
        m_randomAccessibleRow = new IcebergRandomAccessibleRow(getColumns(physicalReadSchema));
        m_rowsRead = 0;
    }

    private static IcebergColumn[] getColumns(final StructType schema) {
        final var columns = new ArrayList<IcebergColumn>();
        final var fields = schema.fields();

        for (var ordinal = 0; ordinal < fields.size(); ordinal++) {
            columns.add(IcebergTypeHelper.getParquetColumn(fields.get(ordinal), ordinal, false));
        }

        return columns.toArray(new IcebergColumn[0]);
    }

    @Override
    public RandomAccessible<IcebergValue> next() throws IOException {
        if (m_recordsIterator.hasNext()) {
            final var row = m_recordsIterator.next();
            m_randomAccessibleRow.setRecord(row);
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
        try {
            m_recordsIterable.close();
        } finally {
            m_fileSystem.close();
        }
    }

}
