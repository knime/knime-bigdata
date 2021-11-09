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
 *   Nov 14, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet.cell;

import static java.util.stream.Collectors.joining;

import java.util.Arrays;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.core.node.context.DeepCopy;

/**
 * Converter for reading list/map format with a repeated group element and one or more value elements:
 *
 * <pre>
 *   required group my_list (LIST) {
 *     repeated group list {
 *       optional binary a (UTF8);
 *       optional binary b (UTF8);
 *       optional binary c (UTF8);
 *     }
 *   }
 * </pre>
 *
 * This converter should be used together with {@link RepeatedGroupParquetCell}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SuppressWarnings("javadoc")
final class RepeatedGroupParquetConverter extends GroupConverter implements ParquetConverterProvider, DeepCopy<RepeatedGroupParquetConverter> {

    private final RepeatedGroupParquetCell[] m_elementConverters;

    RepeatedGroupParquetConverter(final RepeatedGroupParquetCell[] elementConverters) {
        m_elementConverters = elementConverters;
    }

    private RepeatedGroupParquetConverter(final RepeatedGroupParquetConverter toCopy) {
        m_elementConverters = new RepeatedGroupParquetCell[toCopy.m_elementConverters.length];
        for (var i = 0; i < m_elementConverters.length; i++) {
            m_elementConverters[i] = toCopy.m_elementConverters[i].copy();
        }
    }

    /**
     * @return {@link BigDataCell} producers of this convert
     */
    public BigDataCell[] getBigDataCells() {
        return m_elementConverters;
    }

    @Override
    public Converter getConverter(final int fieldIndex) {
        return new GroupConverter() {

            @Override
            public Converter getConverter(final int innerFieldIndex) {
                return m_elementConverters[innerFieldIndex].getConverter();
            }

            @Override
            public void start() {
                for (final var converter : m_elementConverters) {
                    converter.startElement();
                }
            }

            @Override
            public void end() {
                for (final var converter : m_elementConverters) {
                    converter.endElement();
                }
            }

        };
    }

    @Override
    public void start() {
        for (final var converter : m_elementConverters) {
            converter.startRow();
        }
    }

    @Override
    public void end() {
        for (final var converter : m_elementConverters) {
            converter.endRow();
        }
    }

    @Override
    public RepeatedGroupParquetConverter copy() {
        return new RepeatedGroupParquetConverter(this);
    }

    @Override
    public Converter getConverter() {
        return this;
    }

    @Override
    public void reset() {
        for (final var converter : m_elementConverters) {
            converter.reset();
        }
    }

    @Override
    public String toString() {
        return Arrays.stream(m_elementConverters)//
            .map(Object::toString)//
            .collect(joining(", ", "[", "]"));
    }

}
