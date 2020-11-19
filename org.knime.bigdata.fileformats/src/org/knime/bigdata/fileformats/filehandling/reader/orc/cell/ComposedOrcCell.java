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
package org.knime.bigdata.fileformats.filehandling.reader.orc.cell;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.knime.bigdata.fileformats.filehandling.reader.orc.cell.Accesses.BooleanColumnAccess;
import org.knime.bigdata.fileformats.filehandling.reader.orc.cell.Accesses.DoubleColumnAccess;
import org.knime.bigdata.fileformats.filehandling.reader.orc.cell.Accesses.IntColumnAccess;
import org.knime.bigdata.fileformats.filehandling.reader.orc.cell.Accesses.LongColumnAccess;
import org.knime.bigdata.fileformats.filehandling.reader.orc.cell.Accesses.ObjColumnAccess;

/**
 * An {@link OrcCell} that operates on a particular type of {@link ColumnVector} and is composed of a number of accesses
 * that can read from the {@link ColumnVector}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <C> the type of {@link ColumnVector} this instance can read from
 */
final class ComposedOrcCell<C extends ColumnVector> implements OrcCell {

    private static final String NULL_ACCESS_ERROR = "Getters must not be called if isNull returns true.";

    private C m_colVector;

    private int m_idx;

    private final ObjColumnAccess<C, String> m_stringAccess;

    private final DoubleColumnAccess<C> m_doubleAccess;

    private final IntColumnAccess<C> m_intAccess;

    private final LongColumnAccess<C> m_longAccess;

    private final BooleanColumnAccess<C> m_booleanAccess;

    private final Map<Class<?>, ObjColumnAccess<C, ?>> m_objAccessors;

    private ComposedOrcCell(final Builder<C> builder) {
        m_stringAccess = builder.m_stringAccess;
        m_doubleAccess = builder.m_doubleAccess;
        m_intAccess = builder.m_intAccess;
        m_longAccess = builder.m_longAccess;
        m_booleanAccess = builder.m_booleanAccess;
        m_objAccessors = new HashMap<>(builder.m_objAccessors);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setColVector(final ColumnVector colVector) {
        m_colVector = (C)colVector;
    }

    @Override
    public void setIndexInColumn(final int idxInColumn) {
        if (m_colVector.isRepeating) {
            m_idx = 0;
        } else {
            m_idx = idxInColumn;
        }
    }

    @Override
    public boolean isNull() {
        return !m_colVector.noNulls && m_colVector.isNull[m_idx];
    }

    @Override
    public <T> T getObj(final Class<T> expectedClass) {
        assert !isNull() : NULL_ACCESS_ERROR;
        return expectedClass.cast(m_objAccessors.get(expectedClass).getObj(m_colVector, m_idx));
    }

    @Override
    public String getString() {
        assert !isNull() : NULL_ACCESS_ERROR;
        return m_stringAccess.getObj(m_colVector, m_idx);
    }

    @Override
    public int getInt() {
        assert !isNull() : NULL_ACCESS_ERROR;
        return m_intAccess.getInt(m_colVector, m_idx);
    }

    @Override
    public double getDouble() {
        assert !isNull() : NULL_ACCESS_ERROR;
        return m_doubleAccess.getDouble(m_colVector, m_idx);
    }

    @Override
    public long getLong() {
        assert !isNull() : NULL_ACCESS_ERROR;
        return m_longAccess.getLong(m_colVector, m_idx);
    }

    @Override
    public boolean getBoolean() {
        assert !isNull() : NULL_ACCESS_ERROR;
        return m_booleanAccess.getBoolean(m_colVector, m_idx);
    }

    static <C extends ColumnVector> Builder<C> builder(final ObjColumnAccess<C, String> stringAccess) {
        return new Builder<>(stringAccess);
    }

    /**
     * A builder for {@link ComposedOrcCell} objects.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    static final class Builder<C extends ColumnVector> {

        private final ObjColumnAccess<C, String> m_stringAccess;

        private DoubleColumnAccess<C> m_doubleAccess;

        private IntColumnAccess<C> m_intAccess;

        private LongColumnAccess<C> m_longAccess;

        private BooleanColumnAccess<C> m_booleanAccess;

        private Map<Class<?>, ObjColumnAccess<C, ?>> m_objAccessors = new HashMap<>();

        private Builder(final ObjColumnAccess<C, String> stringColAccess) {
            m_stringAccess = stringColAccess;
            m_objAccessors.put(String.class, stringColAccess);
        }

        ComposedOrcCell<C> build() {
            return new ComposedOrcCell<>(this);
        }

        Builder<C> withDoubleAccess(final DoubleColumnAccess<C> access) {
            m_doubleAccess = access;
            m_objAccessors.put(Double.class, access);
            return this;
        }

        Builder<C> withIntAccess(final IntColumnAccess<C> access) {
            m_intAccess = access;
            m_objAccessors.put(Integer.class, access);
            return this;
        }

        Builder<C> withLongAccess(final LongColumnAccess<C> access) {
            m_longAccess = access;
            m_objAccessors.put(Long.class, access);
            return this;
        }

        Builder<C> withBooleanAccess(final BooleanColumnAccess<C> access) {
            m_booleanAccess = access;
            m_objAccessors.put(Boolean.class, access);
            return this;
        }

        <T> Builder<C> withObjAccess(final Class<T> objClass, final ObjColumnAccess<C, T> access) {
            m_objAccessors.put(objClass, access);
            return this;
        }

    }

}
