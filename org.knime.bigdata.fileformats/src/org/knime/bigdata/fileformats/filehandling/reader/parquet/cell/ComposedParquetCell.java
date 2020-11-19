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
 *   Nov 13, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet.cell;

import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.io.api.Converter;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Accesses.BooleanAccess;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Accesses.DoubleAccess;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Accesses.IntAccess;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Accesses.LongAccess;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Accesses.ObjAccess;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.AbstractContainer;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ComposedParquetCell<C extends AbstractContainer<C>> implements ParquetCell {

    private final C m_container;

    private final BooleanAccess<C> m_booleanAccess;

    private final IntAccess<C> m_intAccess;

    private final LongAccess<C> m_longAccess;

    private final DoubleAccess<C> m_doubleAccess;

    private final Map<Class<?>, ObjAccess<C, ?>> m_objAccesses;

    private ComposedParquetCell(final Builder<C> builder) {
        m_container = builder.m_container;
        m_booleanAccess = builder.m_booleanAccess;
        m_intAccess = builder.m_intAccess;
        m_longAccess = builder.m_longAccess;
        m_doubleAccess = builder.m_doubleAccess;
        m_objAccesses = new HashMap<>(builder.m_objAccesses);
    }

    private ComposedParquetCell(final ComposedParquetCell<C> toCopy) {
        m_container = toCopy.m_container.copy();
        m_booleanAccess = toCopy.m_booleanAccess;
        m_intAccess = toCopy.m_intAccess;
        m_longAccess = toCopy.m_longAccess;
        m_doubleAccess = toCopy.m_doubleAccess;
        m_objAccesses = new HashMap<>(toCopy.m_objAccesses);
    }

    static <C extends AbstractContainer<C>> Builder<C> builder(final C container) {
        return new Builder<>(container);
    }

    static final class Builder<C extends AbstractContainer<C>> {

        private final C m_container;

        private BooleanAccess<C> m_booleanAccess;

        private IntAccess<C> m_intAccess;

        private LongAccess<C> m_longAccess;

        private DoubleAccess<C> m_doubleAccess;

        private final Map<Class<?>, ObjAccess<C, ?>> m_objAccesses = new HashMap<>();

        private Builder(final C container) {
            m_container = container;
        }

        ComposedParquetCell<C> build() {
            return new ComposedParquetCell<>(this);
        }

        Builder<C> withIntAccess(final IntAccess<C> intAccess) {
            m_intAccess = intAccess;
            return withObjAccess(Integer.class, intAccess);
        }

        Builder<C> withLongAccess(final LongAccess<C> longAccess) {
            m_longAccess = longAccess;
            return withObjAccess(Long.class, longAccess);
        }

        Builder<C> withBooleanAccess(final BooleanAccess<C> booleanAccess) {
            m_booleanAccess = booleanAccess;
            return withObjAccess(Boolean.class, booleanAccess);
        }

        Builder<C> withDoubleAccess(final DoubleAccess<C> doubleAccess) {
            m_doubleAccess = doubleAccess;
            return withObjAccess(Double.class, doubleAccess);
        }

        <T> Builder<C> withObjAccess(final Class<T> objClass, final ObjAccess<C, T> objAccess) {
            m_objAccesses.put(objClass, objAccess);
            return this;
        }
    }

    @Override
    public boolean isNull() {
        return m_container.isNull();
    }

    @Override
    public String getString() {
        assert !isNull() : "Getters may only be accessed if isNull() returned false.";
        return getObj(String.class);
    }

    @Override
    public <T> T getObj(final Class<T> expectedClass) {
        assert !isNull() : "Getters may only be accessed if isNull() returned false.";
        return expectedClass.cast(m_objAccesses.get(expectedClass).getObj(m_container));
    }

    @Override
    public int getInt() {
        assert !isNull() : "Getters may only be accessed if isNull() returned false.";
        return m_intAccess.getInt(m_container);
    }

    @Override
    public long getLong() {
        assert !isNull() : "Getters may only be accessed if isNull() returned false.";
        return m_longAccess.getLong(m_container);
    }

    @Override
    public boolean getBoolean() {
        assert !isNull() : "Getters may only be accessed if isNull() returned false.";
        return m_booleanAccess.getBoolean(m_container);
    }

    @Override
    public double getDouble() {
        assert !isNull() : "Getters may only be accessed if isNull() returned false.";
        return m_doubleAccess.getDouble(m_container);
    }

    @Override
    public ParquetCell copy() {
        return new ComposedParquetCell<>(this);
    }

    @Override
    public Converter getConverter() {
        return m_container;
    }

    @Override
    public void reset() {
        m_container.reset();
    }

    @Override
    public String toString() {
        return m_container.toString();
    }

}
