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
 *   Nov 5, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.type;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.knime.core.data.DataType;
import org.knime.core.data.blob.BinaryObjectDataCell;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.time.localdate.LocalDateCellFactory;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.core.data.time.localtime.LocalTimeCellFactory;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;

/**
 * Enum of primitive types supported in KNIME.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public enum PrimitiveKnimeType implements KnimeType {
        /**
         * String type.
         */
        STRING("String", StringCell.TYPE, asSet(String.class)),
        /**
         * Boolean type.
         */
        BOOLEAN("Boolean", BooleanCell.TYPE, asSet(Boolean.class)),
        /**
         * Integer type.
         */
        INTEGER("Integer", IntCell.TYPE, asSet(Integer.class)),
        /**
         * Long type.
         */
        LONG("Long", LongCell.TYPE, asSet(Long.class)),
        /**
         * Double type.
         */
        DOUBLE("Double", DoubleCell.TYPE, asSet(Double.class)),
        /**
         * Binary type.
         */
        BINARY("Binary", BinaryObjectDataCell.TYPE, asSet(InputStream.class)),
        /**
         * Local date type.
         */
        DATE("Date", LocalDateCellFactory.TYPE, asSet(LocalDate.class)),
        /**
         * Local time type.
         */
        TIME("Time", LocalTimeCellFactory.TYPE, asSet(LocalTime.class)),
        /**
         * Instant date&time type
         */
        INSTANT_DATE_TIME("Instant Date & Time", ZonedDateTimeCellFactory.TYPE,
            asSet(ZonedDateTime.class, LocalDateTime.class, LocalDate.class)),
        /**
         * Local date&time type i.e. no instant semantics
         */
        LOCAL_DATE_TIME("Local Date & Time", LocalDateTimeCellFactory.TYPE,
            asSet(LocalDateTime.class, ZonedDateTime.class, LocalDate.class));

    private final DataType m_defaultDataType;

    private final Set<Class<?>> m_supportedJavaClasses;

    private final String m_toString;

    private PrimitiveKnimeType(final String toString, final DataType defaultDataType,
        final Set<Class<?>> supportedJavaClasses) {
        m_defaultDataType = defaultDataType;
        m_toString = toString;
        m_supportedJavaClasses = supportedJavaClasses;
    }

    private static Set<Class<?>> asSet(final Class<?>... classes) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(classes)));
    }

    @Override
    public boolean isList() {
        return false;
    }

    @Override
    public ListKnimeType asListType() {
        throw new IllegalStateException("Tried to access a primitive type as list type.");
    }

    @Override
    public DataType getDefaultDataType() {
        return m_defaultDataType;
    }

    @Override
    public String toString() {
        return m_toString;
    }

    @Override
    public PrimitiveKnimeType asPrimitiveType() {
        return this;
    }

    @Override
    public Set<Class<?>> getSupportedJavaClasses() {
        return m_supportedJavaClasses;
    }

}