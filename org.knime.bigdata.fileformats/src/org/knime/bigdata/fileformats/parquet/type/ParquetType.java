///*
// * ------------------------------------------------------------------------
// *
// *  Copyright by KNIME AG, Zurich, Switzerland
// *  Website: http://www.knime.com; Email: contact@knime.com
// *
// *  This program is free software; you can redistribute it and/or modify
// *  it under the terms of the GNU General Public License, Version 3, as
// *  published by the Free Software Foundation.
// *
// *  This program is distributed in the hope that it will be useful, but
// *  WITHOUT ANY WARRANTY; without even the implied warranty of
// *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// *  GNU General Public License for more details.
// *
// *  You should have received a copy of the GNU General Public License
// *  along with this program; if not, see <http://www.gnu.org/licenses>.
// *
// *  Additional permission under GNU GPL version 3 section 7:
// *
// *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
// *  Hence, KNIME and ECLIPSE are both independent programs and are not
// *  derived from each other. Should, however, the interpretation of the
// *  GNU GPL Version 3 ("License") under any applicable laws result in
// *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
// *  you the additional permission to use and propagate KNIME together with
// *  ECLIPSE with only the license terms in place for ECLIPSE applying to
// *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
// *  license terms of ECLIPSE themselves allow for the respective use and
// *  propagation of ECLIPSE together with KNIME.
// *
// *  Additional permission relating to nodes for KNIME that extend the Node
// *  Extension (and in particular that are based on subclasses of NodeModel,
// *  NodeDialog, and NodeView) and that only interoperate with KNIME through
// *  standard APIs ("Nodes"):
// *  Nodes are deemed to be separate and independent programs and to not be
// *  covered works.  Notwithstanding anything to the contrary in the
// *  License, the License does not apply to Nodes, you are not required to
// *  license Nodes under the License, and you are granted a license to
// *  prepare and propagate Nodes, in each case even if such Nodes are
// *  propagated with or for interoperation with KNIME.  The owner of a Node
// *  may freely choose the license terms applicable to such Node, including
// *  when such Node is propagated with or for interoperation with KNIME.
// * ---------------------------------------------------------------------
// *
// * History
// *   23 Apr 2018 (Marc Bux, KNIME AG, Zurich, Switzerland): created
// */
package org.knime.bigdata.fileformats.parquet.type;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.Type;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;

/**
 * An interface for Parquet type classes that can be used to convert data back and forth between KNIME tables and
 * Parquet files. Instances of Parquet type classes have a converter to perform this conversion. Due to these converters
 * having a state, each column within a data table requires its own separate Parquet type instance and, thus, converter.
 * New instances of Parquet type classes can be created using the appropriate {@link ParquetTypeFactory}.
 *
 * @author Marc Bux, KNIME AG, Zurich, Switzerland
 */
public interface ParquetType {
    /**
     * @return the {@link Converter} for converting a Parquet data record of a certain {@link Type} to a KNIME
     * {@link DataCell} of the corresponding {@link DataType}.
     */
    Converter getConverter();

    /**
     * Returns the {@link Type} corresponding to this {@link ParquetType}.
     *
     * @return the {@link Type} corresponding to this {@link ParquetType}
     */
    Type getParquetType();

    /**
     * Method for obtaining the {@link DataCell} created from the last value passed to this {@link ParquetType}'s
     * converter.
     *
     * @return the {@link DataCell} created from the last observed value
     */
    DataCell readValue();

    /**
     * Method for writing the value contained within a {@link DataCell} to a {@link RecordConsumer} that consumes the
     * value and writes it to a Parquet file.
     *
     * @param consumer the {@link RecordConsumer} that consumes (and writes) the current batch of rows to a Parquet file
     * @param cell the {@link DataCell} whose value is to be written to a Parquet file
     * @param index the column index of the written {@link DataCell}
     */
    void writeValue(RecordConsumer consumer, DataCell cell, int index);
}
