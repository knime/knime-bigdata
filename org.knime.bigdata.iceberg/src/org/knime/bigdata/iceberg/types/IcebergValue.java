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
package org.knime.bigdata.iceberg.types;

import org.apache.iceberg.data.Record;
import org.knime.bigdata.iceberg.nodes.reader.mapper.IcebergReadAdapterFactory;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TypeHierarchy;

/**
 * Column view that delegates to a delta {@link Record} of a {@link IcebergRandomAccessibleRow}.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public abstract class IcebergValue {

    protected final IcebergRandomAccessibleRow m_randomAccessibleRow;

    protected final int m_ordinal;

    IcebergValue(final IcebergRandomAccessibleRow randomAccessibleRow, final int ordinal) {
        m_randomAccessibleRow = randomAccessibleRow;
        m_ordinal = ordinal;
    }

    /**
     * Indicates whether the underlying value is {@code null}.</br>
     * If this method returns {@code true}, none of the getters must be called.
     *
     * @return {@code true} if the underlying value is {@code null}
     */
    public boolean isNull() {
        return m_randomAccessibleRow.getRecord().get(m_ordinal) == null;
    }

    /**
     * Gets the underlying value as boolean.
     *
     * @return the underlying value as boolean
     * @throws UnsupportedOperationException if the underlying value is incompatible with boolean
     */
    public boolean getBoolean() {
        throw unsupported("getBoolean", getClass().getSimpleName());
    }

    /**
     * Gets the underlying value as int.
     *
     * @return the underlying value as int
     * @throws UnsupportedOperationException if the underlying value is incompatible with int
     */
    public int getInt() {
        throw unsupported("getInt", getClass().getSimpleName());
    }

    /**
     * Gets the underlying value as long.
     *
     * @return the underlying value as long
     * @throws UnsupportedOperationException if the underlying value is incompatible with long
     */
    public long getLong() {
        throw unsupported("getLong", getClass().getSimpleName());
    }

    /**
     * Gets the underlying value as double.
     *
     * @return the underlying value as double
     * @throws UnsupportedOperationException if the underlying value is incompatible with double
     */
    public double getDouble() {
        throw unsupported("getDouble", getClass().getSimpleName());
    }

    /**
     * Returns a String representing the underlying value. All cell implementations must implement this getter.
     *
     * @return the underlying value as String
     */
    public abstract String getString();

    /**
     * Returns the underlying value as instance of {@link Class expectedClass}.</br>
     * Which types have to be supported depends on which {@link IcebergDataType} this value reader is compatible
     * with.</br>
     * Example: Let's assume that this cell is compatible to {@link IcebergPrimitiveType#INTEGER}, then it needs to
     * support all {@link IcebergPrimitiveType#getSupportedJavaClasses()} as well as all supported Java classes of all super types
     * according to the used {@link TypeHierarchy}. In case we use {@link IcebergReadAdapterFactory#TYPE_HIERARCHY} the super
     * types would be {@link IcebergPrimitiveType#LONG}, {@link IcebergPrimitiveType#DOUBLE} and
     * {@link IcebergPrimitiveType#STRING}.
     *
     * @param <T> the expected type of the return value
     * @param expectedClass the expected class of the underlying value
     * @return the underlying value as instance of {@link Class expectedClass}
     * @throws UnsupportedOperationException if the underlying value is incompatible with expectedClass
     */
    public abstract <T> T getObj(Class<T> expectedClass);

    @Override
    public String toString() {
        return getString();
    }

    static UnsupportedOperationException unsupported(final String methodName, final String simpleClassName) {
        return new UnsupportedOperationException(
            String.format("The method '%s' is not supported by %s.", methodName, simpleClassName));
    }

}
