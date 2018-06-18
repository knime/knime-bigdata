/*
 * ------------------------------------------------------------------------
 *
 * Copyright by KNIME AG, Zurich, Switzerland Website: http://www.knime.com;
 * Email: contact@knime.com
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, Version 3, as published by the
 * Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, see <http://www.gnu.org/licenses>.
 *
 * Additional permission under GNU GPL version 3 section 7:
 *
 * KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs. Hence,
 * KNIME and ECLIPSE are both independent programs and are not derived from each
 * other. Should, however, the interpretation of the GNU GPL Version 3
 * ("License") under any applicable laws result in KNIME and ECLIPSE being a
 * combined program, KNIME AG herewith grants you the additional permission to
 * use and propagate KNIME together with ECLIPSE with only the license terms in
 * place for ECLIPSE applying to ECLIPSE and the GNU GPL Version 3 applying for
 * KNIME, provided the license terms of ECLIPSE themselves allow for the
 * respective use and propagation of ECLIPSE together with KNIME.
 *
 * Additional permission relating to nodes for KNIME that extend the Node
 * Extension (and in particular that are based on subclasses of NodeModel,
 * NodeDialog, and NodeView) and that only interoperate with KNIME through
 * standard APIs ("Nodes"): Nodes are deemed to be separate and independent
 * programs and to not be covered works. Notwithstanding anything to the
 * contrary in the License, the License does not apply to Nodes, you are not
 * required to license Nodes under the License, and you are granted a license to
 * prepare and propagate Nodes, in each case even if such Nodes are propagated
 * with or for interoperation with KNIME. The owner of a Node may freely choose
 * the license terms applicable to such Node, including when such Node is
 * propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History Jan 23, 2018 (dietzc): created
 */
package org.knime.bigdata.fileformats.orc.types;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.orc.TypeDescription;
import org.knime.bigdata.fileformats.orc.types.OrcStringTypeFactory.OrcStringType;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.StringValue;
import org.knime.core.data.def.StringCell;

/**
 * @author dietzc
 */
public class OrcStringTypeFactory implements OrcTypeFactory<OrcStringType> {

    /**
     * {@inheritDoc}
     */
    @Override
    public OrcStringType create() {
        return new OrcStringType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType type() {
        return StringCell.TYPE;
    }

    /**
     * ORC type for string values
     */
    public static class OrcStringType extends AbstractOrcType<BytesColumnVector> {
        /**
         * Creates an ORC type for string values
         */
        public OrcStringType() {
            super(TypeDescription.createString());
        }

        @Override
        public void writeValueNonNull(final BytesColumnVector columnVector, final int rowInBatch, final DataCell cell) {
            final byte[] b = ((StringValue) cell).getStringValue().getBytes(StandardCharsets.UTF_8);
            columnVector.ensureSize(b.length, true);
            columnVector.setRef(rowInBatch, b, 0, b.length);
        }

        @Override
        public DataCell readValueNonNull(final BytesColumnVector columnVector, final int rowInBatchOrZero) {
            return new StringCell(
                    new String(columnVector.vector[rowInBatchOrZero], columnVector.start[rowInBatchOrZero],
                            columnVector.length[rowInBatchOrZero], StandardCharsets.UTF_8));
        }

        /**
         * Writes a given String to the column vector
         *
         * @param columnVector the column vector to write to
         * @param rowInBatch the row to write
         * @param string the String to write
         */
        public static final void writeString(final BytesColumnVector columnVector, final int rowInBatch,
                final String string) {
            final BytesColumnVector byteVectorColumn = columnVector;
            if (string == null) {
                byteVectorColumn.isNull[rowInBatch] = true;
            } else {
                final byte[] b = string.getBytes(StandardCharsets.UTF_8);
                byteVectorColumn.setRef(rowInBatch, b, 0, b.length);
            }
        }

        /**
         * Reads a String from the column vector
         *
         * @param byteVectorColumn the column vector to read from
         * @param rowInBatch the row to write
         * @return the string
         */
        public static String readString(final BytesColumnVector byteVectorColumn, final int rowInBatch) {
            if (byteVectorColumn.noNulls) {
                if (byteVectorColumn.isRepeating) {
                    return new String(byteVectorColumn.vector[0], StandardCharsets.UTF_8);
                } else if (byteVectorColumn.isNull[rowInBatch]) {
                    return null;
                } else {
                    return new String(byteVectorColumn.vector[rowInBatch], byteVectorColumn.start[rowInBatch],
                            byteVectorColumn.length[rowInBatch], StandardCharsets.UTF_8);
                }
            }
            return null;
        }
    }
}
