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
 *   Apr 6, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;

import org.apache.commons.lang3.NotImplementedException;
import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.ListKnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.PrimitiveKnimeType;
import org.knime.core.columnar.data.DataSpec;
import org.knime.filehandling.core.node.table.reader.ftrf.adapter.batch.ValueAccess;
import org.knime.filehandling.core.node.table.reader.ftrf.adapter.batch.ValueAccessFactory;
import org.knime.filehandling.core.node.table.reader.ftrf.adapter.batch.ValueAccess.DefaultObjectAccess;

public enum BigDataValueAccessFactory implements ValueAccessFactory<KnimeType> {
        INSTANCE;

    @Override
    public ValueAccess createValueAccess(final KnimeType type) {
        if (type.isList()) {
            return getListAccess(type.asListType());
        } else {
            return getPrimitiveAccess(type.asPrimitiveType());
        }
    }

    private static ValueAccess getListAccess(final ListKnimeType type) {
        throw new NotImplementedException("List support isn't implemented, yet.");
    }

    private static ValueAccess getPrimitiveAccess(final PrimitiveKnimeType type) {
        switch (type) {
            case BINARY:
                throw new NotImplementedException("Binary support isn't implemented, yet");
            case BOOLEAN:
                return (ValueAccess.BooleanAccess<BigDataCell>)BigDataCell::getBoolean;
            case DATE:
                return createObjectAccess(LocalDate.class);
            case DOUBLE:
                return (ValueAccess.DoubleAccess<BigDataCell>)BigDataCell::getDouble;
            case INSTANT_DATE_TIME:
                return createObjectAccess(ZonedDateTime.class);
            case INTEGER:
                return (ValueAccess.IntAccess<BigDataCell>)BigDataCell::getInt;
            case LOCAL_DATE_TIME:
                return createObjectAccess(LocalDateTime.class);
            case LONG:
                return (ValueAccess.LongAccess<BigDataCell>)BigDataCell::getLong;
            case STRING:
                return createObjectAccess(String.class);
            case TIME:
                return createObjectAccess(LocalTime.class);
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);

        }
    }

    private static <O> DefaultObjectAccess<O, BigDataCell> createObjectAccess(final Class<O> expectedClass) {
        return new DefaultObjectAccess<>(expectedClass, c -> c.getObj(expectedClass));
    }

    @Override
    public DataSpec getDataSpec(final KnimeType type) {

        if (type.isList()) {
            return getListSpec(type.asListType());
        } else {
            return getPrimitiveSpec(type.asPrimitiveType());
        }
    }

    private static DataSpec getListSpec(final ListKnimeType type) {
        throw new NotImplementedException("List support isn't implemented, yet.");
    }

    private static DataSpec getPrimitiveSpec(final PrimitiveKnimeType type) {
        switch (type) {
            case BINARY:
                throw new NotImplementedException("Binary support isn't implemented, yet");
            case BOOLEAN:
                return DataSpec.booleanSpec();
            case DATE:
                return DataSpec.localDateSpec();
            case DOUBLE:
                return DataSpec.doubleSpec();
            case INSTANT_DATE_TIME:
                return DataSpec.zonedDateTimeSpec();
            case INTEGER:
                return DataSpec.intSpec();
            case LOCAL_DATE_TIME:
                return DataSpec.localDateTimeSpec();
            case LONG:
                return DataSpec.longSpec();
            case STRING:
                return DataSpec.stringSpec();
            case TIME:
                return DataSpec.localTimeSpec();
            default:
                throw new IllegalArgumentException("Unsupported primitive type: " + type);
        }
    }
}