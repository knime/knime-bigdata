/*
 * ------------------------------------------------------------------------
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
 * -------------------------------------------------------------------
 *
 * History Mar 18, 2016 (wiswedel): created
 */
package org.knime.bigdata.fileformats.orc.types;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.TypeDescription;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;

/**
 * Abstract implementation for the ORC type interface.
 *
 * @author Bernd Wiswedel, KNIME AG, Zuerich, Switzerland
 * @param <C> The type of the handled ColumnVector
 */
public abstract class AbstractOrcType<C extends ColumnVector> implements OrcType<C> {

    private final TypeDescription m_typeDescription;

    /**
     * Creates an ORC type with the given type description.
     * 
     * @param typeDescription the ORC type description
     */
    public AbstractOrcType(final TypeDescription typeDescription) {
        m_typeDescription = typeDescription;
    }

    @Override
    public void writeValue(final C columnVector, final int rowInBatch, final DataCell cell) {
        if (cell.isMissing()) {
            columnVector.noNulls = false;
            columnVector.isNull[rowInBatch] = true;
        } else {
            writeValueNonNull(columnVector, rowInBatch, cell);
        }
    }

    @Override
    public DataCell readValue(final C columnVector, final int rowInBatch) {
        final int rowInBatchCorrected = columnVector.isRepeating ? 0 : rowInBatch;
        if (columnVector.noNulls || !columnVector.isNull[rowInBatchCorrected]) {
            return readValueNonNull(columnVector, rowInBatchCorrected);
        }
        return DataType.getMissingCell();
    }

    /**
     * Reads a row from the column vector into a {@link DataCell}.
     * 
     * @param columnVector the column vector to read from
     * @param rowInBatchCorrected the row to read
     * @return DataCell the KNIME Data Cell containing the read data
     */
    protected abstract DataCell readValueNonNull(C columnVector, int rowInBatchCorrected);

    /**
     * Writes the content of the {@link DataCell} into the column vector at the
     * given row.
     * 
     * @param columnVector the column vector to write into
     * @param rowInBatch the row to write into
     * @param cell the cell to write
     */
    protected abstract void writeValueNonNull(C columnVector, int rowInBatch, DataCell cell);

    @Override
    public final TypeDescription getTypeDescription() {
        return m_typeDescription;
    }

}
