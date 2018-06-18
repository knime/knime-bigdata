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
 * History 24 Apr 2018 (Marc Bux, KNIME AG, Zurich, Switzerland): created
 */
package org.knime.bigdata.fileformats.parquet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.knime.bigdata.fileformats.parquet.type.BinaryParquetTypeFactory;
import org.knime.bigdata.fileformats.parquet.type.BooleanParquetTypeFactory;
import org.knime.bigdata.fileformats.parquet.type.DoubleParquetTypeFactory;
import org.knime.bigdata.fileformats.parquet.type.IntParquetTypeFactory;
import org.knime.bigdata.fileformats.parquet.type.LongParquetTypeFactory;
import org.knime.bigdata.fileformats.parquet.type.ParquetType;
import org.knime.bigdata.fileformats.parquet.type.ParquetTypeFactory;
import org.knime.bigdata.fileformats.parquet.type.StringParquetTypeFactory;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.blob.BinaryObjectDataCell;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;

/**
 * A class for storing KNIME tables as columnar Parquet files.
 *
 * @author Mareike Hoeger, KNIME AG, Zurich, Switzerland
 * @author Marc Bux, KNIME AG, Zurich, Switzerland
 */
public final class ParquetTableStoreFormat {

    private ParquetTableStoreFormat() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * The scheme (if any) to be used for compressing columns within Parquet
     * files.
     */
    public static final CompressionCodecName DEFAULT_PARQUET_COMPRESSION_CODEC = CompressionCodecName.SNAPPY;

    /**
     * Flag that determines whether Parquet files are to hold a dictionary.
     */
    public static final boolean DEFAULT_PARQUET_IS_DICTIONARY_ENABLED = true;

    /**
     * Parquet horizontally divides tables into row groups. A row group is a
     * logical horizontal partitioning of the data into rows. This parameter
     * determines the size of a row group (in Bytes). Row groups are flushed to
     * the file when the current size of the buffer (held in memory) approaches
     * the specified size.
     */
    public static final int DEFAULT_PARQUET_ROW_GROUP_SIZE = 128 * 1024 * 1024;

    /**
     * Parquet vertically divides row groups into column chunks. A Column chunk
     * is then further divided horizontally into pages. This parameter
     * determines the size of a page (in Bytes).
     */
    public static final int DEFAULT_PARQUET_PAGE_SIZE = ParquetProperties.DEFAULT_PAGE_SIZE;

    /**
     * The key under which to store row keys in the Parquet file.
     */
    public static final String PARQUET_SCHEMA_ROWKEY = "row";

    /*
     * Factories for the KNIME data types currently supported to be written to / read from Parquet files.
     */
    private static final Map<DataType, ParquetTypeFactory> TYPE_FACTORIES = new HashMap<>();

    static {
        TYPE_FACTORIES.put(IntCell.TYPE, new IntParquetTypeFactory());
        TYPE_FACTORIES.put(LongCell.TYPE, new LongParquetTypeFactory());
        TYPE_FACTORIES.put(DoubleCell.TYPE, new DoubleParquetTypeFactory());
        TYPE_FACTORIES.put(StringCell.TYPE, new StringParquetTypeFactory());
        TYPE_FACTORIES.put(BooleanCell.TYPE, new BooleanParquetTypeFactory());
        TYPE_FACTORIES.put(BinaryObjectDataCell.TYPE, new BinaryParquetTypeFactory());
    }

    /**
     * Helper method for creating {@link ParquetType} instances, each
     * corresponding to a {@link DataType} of a column specified in a
     * {@link DataTableSpec}.
     *
     * @param spec specification of a KNIME table
     * @return an array of {@link ParquetType} instances according to the types
     *         of a KNIME table's columns
     */
    public static ParquetType[] parquetTypesFromSpec(final DataTableSpec spec) {
        return spec.stream().map(c -> TYPE_FACTORIES.get(c.getType()).create(c.getName())).toArray(ParquetType[]::new);
    }

    /**
     * Returns an array of all unsupported types for a given table spec
     *
     * @param spec the table spec to check
     * @return the array containing the unsupported types
     */
    public static String[] getUnsupportedTypes(final DataTableSpec spec) {
        final List<String> unsupported = spec.stream().map(DataColumnSpec::getType)
                .filter(t -> !TYPE_FACTORIES.containsKey(t)).map(DataType::toString).collect(Collectors.toList());

        return unsupported.toArray(new String[unsupported.size()]);
    }

    /**
     * Check if this format can write a table with the argument spec.
     *
     * @param spec non-null spec that is about to be written
     * @return true if possible, false if not (will log a message and then use
     *         the fallback format)
     */
    public static boolean accepts(final DataTableSpec spec) {
        return spec.stream().map(DataColumnSpec::getType).allMatch(TYPE_FACTORIES::containsKey);
    }

}
