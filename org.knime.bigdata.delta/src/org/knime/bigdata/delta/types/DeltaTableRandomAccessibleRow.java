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
package org.knime.bigdata.delta.types;

import java.util.Arrays;

import org.knime.bigdata.delta.util.DeltaTableFileFormatException;
import org.knime.filehandling.core.node.table.reader.randomaccess.AbstractRandomAccessible;

import io.delta.kernel.data.Row;

/**
 * Reuseable random accessible implementation that delegates via a {@link DeltaTableValue} to a delta {@link Row}.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public class DeltaTableRandomAccessibleRow extends AbstractRandomAccessible<DeltaTableValue> {

    private final DeltaTableValue[] m_valueReader;

    private Row m_row;

    public DeltaTableRandomAccessibleRow(final DeltaTableColumn[] columns) {
        m_valueReader = Arrays.stream(columns) //
            .map(col -> createValueReader(this, col)) //
            .toList().toArray(new DeltaTableValue[0]);
    }

    private static DeltaTableValue createValueReader(final DeltaTableRandomAccessibleRow row,
        final DeltaTableColumn col) {

        if (col instanceof DeltaTablePrimitiveColumn primitiveColumn) {
            return primitiveColumn.getValueReader(row);
        } else if (col instanceof DeltaTableArrayColumn listColumn) {
            return listColumn.getValueReader(row);
        } else {
            throw new DeltaTableFileFormatException(
                String.format("Unsupported structure or type: %s", col.getClass().getSimpleName()));
        }
    }

    public void setRow(final Row row) {
        m_row = row;
    }

    Row getRow() {
        return m_row;
    }

    @Override
    public int size() {
        return m_valueReader.length;
    }

    @Override
    public DeltaTableValue get(final int idx) {
        return m_valueReader[idx];
    }

}
