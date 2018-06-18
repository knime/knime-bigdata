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
 * History 30.04.2018 ("Mareike Hoeger, KNIME GmbH, Konstanz, Germany"): created
 */
package org.knime.bigdata.fileformats.orc.types;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Optional;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.orc.TypeDescription;
import org.knime.bigdata.fileformats.orc.types.OrcBinaryTypeFactory.OrcBinaryType;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataCellDataOutput;
import org.knime.core.data.DataCellSerializer;
import org.knime.core.data.DataType;
import org.knime.core.data.DataTypeRegistry;
import org.knime.core.data.blob.BinaryObjectCellFactory;
import org.knime.core.data.blob.BinaryObjectDataCell;

/**
 * Factory for the ORC binary.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcBinaryTypeFactory implements OrcTypeFactory<OrcBinaryType> {

    /**
     * {@inheritDoc}
     */
    @Override
    public OrcBinaryType create() {

        return new OrcBinaryType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType type() {
        return BinaryObjectDataCell.TYPE;
    }

    /**
     * Orc Type for Binary field
     *
     * @author "Mareike Hoeger, KNIME GmbH, Konstanz, Germany"
     */
    public static class OrcBinaryType extends AbstractOrcType<BytesColumnVector> {

        /**
         * Creates a binary ORC type
         */
        public OrcBinaryType() {
            super(TypeDescription.createBinary());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected DataCell readValueNonNull(final BytesColumnVector columnVector, final int rowInBatchCorrected) {
            final BinaryObjectCellFactory cellFactory = new BinaryObjectCellFactory();
            try {
                return cellFactory.create(columnVector.vector[rowInBatchCorrected]);
            } catch (final IOException ex) {
                throw new BigDataFileFormatException(ex);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void writeValueNonNull(final BytesColumnVector columnVector, final int rowInBatch,
                final DataCell cell) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (final DataCellWriteObjectOutputStream cellOutStream = new DataCellWriteObjectOutputStream(out)) {
                cellOutStream.writeDataCell(cell);
                columnVector.vector[rowInBatch] = out.toByteArray();
            } catch (final IOException ex) {
                throw new BigDataFileFormatException(ex);
            }

        }

        /** Output stream used for cloning a data cell. */
        private static final class DataCellWriteObjectOutputStream extends ObjectOutputStream
                implements DataCellDataOutput {

            /**
             * Call super.
             *
             * @param out To delegate
             * @throws IOException If super throws it.
             */
            DataCellWriteObjectOutputStream(final OutputStream out) throws IOException {
                super(out);
            }

            /** {@inheritDoc} */
            @Override
            public void writeDataCell(final DataCell cell) throws IOException {
                writeUTF(cell.getClass().getName());
                final Optional<DataCellSerializer<DataCell>> cellSerializer = DataTypeRegistry.getInstance()
                        .getSerializer(cell.getClass());
                if (cellSerializer.isPresent()) {
                    cellSerializer.get().serialize(cell, this);
                } else {
                    writeObject(cell);
                }
            }

        }
    }
}
