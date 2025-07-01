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
 *   Nov 8, 2021 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.iceberg.types;

import java.util.function.Function;

import org.knime.filehandling.core.node.table.reader.spec.TypedReaderTableSpec.TypedReaderTableSpecBuilder;

/**
 * Represents a column with an array of values.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class IcebergArrayColumn extends IcebergColumn {

    private final String m_name;
    private final IcebergListType m_dataType;
    private final Function<IcebergRandomAccessibleRow, IcebergValue> m_createValueReader;

    IcebergArrayColumn(final IcebergPrimitiveType elementType, final String name,
        final Function<IcebergRandomAccessibleRow, IcebergValue> createValueReader) {
        m_dataType = IcebergListType.of(elementType);
        m_name = name;
        m_createValueReader = createValueReader;
    }

    /**
     * Create a value reader instance.
     *
     * @param row the accessible row with the data to read
     * @return the value reader
     */
    public IcebergValue getValueReader(final IcebergRandomAccessibleRow row) {
        return m_createValueReader.apply(row);
    }

    /**
     * @return {@link IcebergDataType} of this column
     */
    public IcebergDataType getDataType() {
        return m_dataType;
    }

    /**
     * @return name of this column
     */
    public String getName() {
        return m_name;
    }

    @Override
    public void addColumnSpecs(final TypedReaderTableSpecBuilder<IcebergDataType> specBuilder) {
        specBuilder.addColumn(m_name, m_dataType, true);
    }

}
