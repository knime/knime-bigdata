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

import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;

/**
 * Factory for {@link OrcCell} objects.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class OrcCellFactory {

    private OrcCellFactory() {
        // static factory class
    }

    /**
     * Creates an {@link OrcCell} for the provided {@link TypeDescription column}.
     *
     * @param column in an ORC schema
     * @return a {@link OrcCell} for accessing the described column
     */
    public static OrcCell create(final TypeDescription column) {//NOSONAR really?
        final Category category = column.getCategory();
        switch (category) {
            case DECIMAL:
                return createForDecimalColumnVector();
            // types stored in DoubleColumnVector
            case FLOAT:
            case DOUBLE:
                return createForDoubleColumnVector();
            // types stored in LongColumnVector
            case BOOLEAN:
                return createBooleanCell();
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return createForLongColumnVector(category);
            case CHAR:
            case STRING:
            case VARCHAR:
                return createForBytesColumnVector();
            case LIST:
                return createForListColumnVector(column);
            case BINARY:
                return createForBinaryColumn();
            case DATE:
                return createLocalDateCell();
            case TIMESTAMP:
                return createForTimeStampColumn();
            // unsupported types
            case MAP:
            case STRUCT:
            case UNION:
            default:
                throw new IllegalArgumentException(String.format("The type %s is not supported.", category));
        }
    }

    private static ComposedOrcCell<LongColumnVector> createLocalDateCell() {
        return ComposedOrcCell.builder(Accesses::getLocalDateString)//
            .withObjAccess(LocalDate.class, Accesses::getLocalDate)//
            .withObjAccess(LocalDateTime.class, Accesses::getLocalDateTime).build();
    }

    private static ComposedOrcCell<TimestampColumnVector> createForTimeStampColumn() {
        return ComposedOrcCell.builder(Accesses::getZonedDateTimeString)//
            .withObjAccess(LocalDateTime.class, Accesses::getLocalDateTime)//
            .withObjAccess(ZonedDateTime.class, Accesses::getZonedDateTime).build();
    }

    private static ComposedOrcCell<BytesColumnVector> createForBinaryColumn() {
        return ComposedOrcCell.builder(Accesses::getBinaryString)//
            .withObjAccess(InputStream.class, Accesses::getInputStream)//
            .build();
    }

    private static OrcCell createForListColumnVector(final TypeDescription listColumn) {
        final TypeDescription elementType = listColumn.getChildren().get(0);
        OrcCell elementCell = create(elementType);
        return new ListOrcCell(elementCell);
    }

    private static OrcCell createForBytesColumnVector() {
        final ComposedOrcCell.Builder<BytesColumnVector> builder = ComposedOrcCell.builder(Accesses::getString);
        return builder.build();
    }

    private static OrcCell createForDecimalColumnVector() {
        final ComposedOrcCell.Builder<DecimalColumnVector> builder = ComposedOrcCell.builder(Accesses::getString);
        return builder.withDoubleAccess(Accesses::getDouble).build();
    }

    private static OrcCell createForDoubleColumnVector() {
        final ComposedOrcCell.Builder<DoubleColumnVector> builder = ComposedOrcCell.builder(Accesses::getString);
        return builder.withDoubleAccess(Accesses::getDouble).build();
    }

    private static OrcCell createBooleanCell() {
        final ComposedOrcCell.Builder<LongColumnVector> builder = ComposedOrcCell.builder(Accesses::getStringBoolean);
        return builder.withBooleanAccess(Accesses::getBoolean).build();
    }

    private static OrcCell createForLongColumnVector(final Category category) {
        final ComposedOrcCell.Builder<LongColumnVector> builder = ComposedOrcCell.builder(Accesses::getString);
        switch (category) {
            case BYTE:
            case SHORT:
            case INT:// NOSONAR falling through to long is intended
                builder.withIntAccess(Accesses::getInt);
            case LONG:
                builder.withLongAccess(Accesses::getLong)//
                    .withDoubleAccess(Accesses::getDouble).withObjAccess(LocalDate.class, Accesses::getLocalDate);
                break;
            default:
                throw new IllegalStateException("Coding-error: Non-integer type encountered: " + category);
        }
        return builder.build();
    }

}
