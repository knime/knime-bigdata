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
 *   Apr 8, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.orc;

import java.time.LocalDateTime;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.knime.core.columnar.ColumnarSchema;
import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.DataSpec;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.ListData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.LocalDateTimeData.LocalDateTimeReadData;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;

/**
 * Utility class dealing with ORC types and vectors.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class OrcTypeUtils {

    private OrcTypeUtils() {
        // static utility class
    }

    static ColumnarSchema convert(final TypeDescription schema) {
        return new OrcColumnarSchema(schema);
    }


    private static final class OrcColumnarSchema implements ColumnarSchema {

        private final DataSpec[] m_specs;

        OrcColumnarSchema(final TypeDescription schema) {
            m_specs = schema.getChildren().stream()//
                .map(OrcColumnarSchema::toDataSpec)//
                .toArray(DataSpec[]::new);
        }

        private static DataSpec toDataSpec(final TypeDescription type) {//NOSONAR
            final Category category = type.getCategory();
            switch (category) {
                case BINARY:
                    return DataSpec.varBinarySpec();
                case BOOLEAN:
                    return DataSpec.booleanSpec();
                case FLOAT:
                case DECIMAL:
                case DOUBLE:
                    return DataSpec.doubleSpec();
                case LIST:
                    return new ListData.ListDataSpec(toDataSpec(type.getChildren().get(0)));
                case LONG:
                    return DataSpec.longSpec();
                case INT:
                case BYTE:
                case SHORT:
                    return DataSpec.intSpec();
                case CHAR:
                case STRING:
                case VARCHAR:
                    return DataSpec.stringSpec();
                case TIMESTAMP:
                    return DataSpec.localDateTimeSpec();
                case DATE:
                    return DataSpec.localDateSpec();
                case UNION:
                case STRUCT: // TODO add struct support?
                case MAP:
                default:
                    throw new IllegalArgumentException(String.format("The ORC type '%s' is not supported.", category));
            }
        }

        @Override
        public int numColumns() {
            return m_specs.length;
        }

        @Override
        public DataSpec getSpec(final int index) {
            return m_specs[index];
        }

    }

    static NullableReadData convert(final ColumnVector vector, final int length) {
        return convert(vector, 0, length);
    }

    private static NullableReadData convert(final ColumnVector vector, final int offset, final int length) {
        if (vector instanceof LongColumnVector) {
            return new LongColumnReadData((LongColumnVector)vector, offset, length);
        } else if (vector instanceof DoubleColumnVector) {
            return new DoubleColumnReadData((DoubleColumnVector) vector, offset, length);
        } else if (vector instanceof DecimalColumnVector) {
            return new DecimalColumnReadData((DecimalColumnVector)vector, offset, length);
        } else if (vector instanceof BytesColumnVector) {
            return new BytesColumnReadData((BytesColumnVector)vector, offset, length);
        } else if (vector instanceof TimestampColumnVector) {
            return new TimestampColumnReadData((TimestampColumnVector)vector, offset, length);
        } else if (vector instanceof ListColumnVector) {
            return new ListColumnReadData((ListColumnVector)vector, offset, length);
        } else {
            throw new IllegalArgumentException("Unsupported column vector class: " + vector.getClass());
        }
    }

    private abstract static class OrcColumnReadData<V extends ColumnVector> implements NullableReadData {

        private final int m_length;

        private final int m_offset;

        protected final V m_vector;

        OrcColumnReadData(final V vector, final int offset, final int length) {
            m_length = length;
            m_vector = vector;
            m_offset = offset;
        }

        protected final int adjustIndex(final int index) {
            return m_vector.isRepeating ? 0 : (index + m_offset);
        }

        @Override
        public boolean isMissing(final int index) {
            return !m_vector.noNulls && m_vector.isNull[index];
        }

        @Override
        public final int length() {
            return m_length;
        }

        @Override
        public void retain() {
            // TODO Auto-generated method stub

        }

        @Override
        public void release() {
            // TODO Auto-generated method stub

        }

        @Override
        public long sizeOf() {
            // TODO Auto-generated method stub
            return 0;
        }

    }

    private static final class LongColumnReadData extends OrcColumnReadData<LongColumnVector>
        implements IntReadData, LongReadData, BooleanReadData {

        LongColumnReadData(final LongColumnVector vector, final int offset, final int length) {
            super(vector, offset, length);
        }

        @Override
        public long getLong(final int index) {
            return m_vector.vector[adjustIndex(index)];
        }

        @Override
        public int getInt(final int index) {
            final long value = getLong(index);
            return (int)value;
        }

        @Override
        public boolean getBoolean(final int index) {
            return getLong(index) == 1L;
        }

    }

    private static final class DoubleColumnReadData extends OrcColumnReadData<DoubleColumnVector>
        implements DoubleReadData {

        DoubleColumnReadData(final DoubleColumnVector vector, final int offset, final int length) {
            super(vector, offset, length);
        }

        @Override
        public double getDouble(final int index) {
            return m_vector.vector[adjustIndex(index)];
        }

    }

    private static final class DecimalColumnReadData extends OrcColumnReadData<DecimalColumnVector>
        implements DoubleReadData {

        DecimalColumnReadData(final DecimalColumnVector vector, final int offset, final int length) {
            super(vector, offset, length);
        }

        @Override
        public double getDouble(final int index) {
            return m_vector.vector[adjustIndex(index)].doubleValue();
        }

    }

    private static final class BytesColumnReadData extends OrcColumnReadData<BytesColumnVector>
        implements StringReadData, VarBinaryReadData {

        BytesColumnReadData(final BytesColumnVector vector, final int offset, final int length) {
            super(vector, offset, length);
        }

        @Override
        public String getString(final int index) {
            // BytesColumnVector takes care of the isRepeating property
            return m_vector.toString(index);
        }

        @Override
        public byte[] getBytes(final int index) {
            final int repeatingAwareIndex = adjustIndex(index);
            final int start = m_vector.start[repeatingAwareIndex];
            final int length = m_vector.length[repeatingAwareIndex];
            return Arrays.copyOfRange(m_vector.vector[repeatingAwareIndex], start, start + length);
        }

    }

    private static final class TimestampColumnReadData extends OrcColumnReadData<TimestampColumnVector> implements LocalDateTimeReadData {

        TimestampColumnReadData(final TimestampColumnVector vector, final int offset, final int length) {
            super(vector, offset, length);
        }

        @Override
        public LocalDateTime getLocalDateTime(final int index) {
            return m_vector.asScratchTimestamp(adjustIndex(index)).toLocalDateTime();
        }

    }

    private static final class ListColumnReadData extends OrcColumnReadData<ListColumnVector> implements ListReadData {

        ListColumnReadData(final ListColumnVector vector, final int offset, final int length) {
            super(vector, offset, length);
        }

        @Override
        public <C extends NullableReadData> C createReadData(final int index) {
            final long offset = m_vector.offsets[index];
            final long length = m_vector.lengths[index];
            assert length < Integer.MAX_VALUE : "List is too long.";
            assert offset < Integer.MAX_VALUE : "Offset larger than Integer.MAX_VALUE are not supported";
            @SuppressWarnings("unchecked")
            C readData = (C)convert(m_vector.child, (int)offset, (int)length);
            return readData;
        }

    }

}
