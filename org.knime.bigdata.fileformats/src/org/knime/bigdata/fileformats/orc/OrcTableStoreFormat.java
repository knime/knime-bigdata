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
 * History Mar 14, 2016 (wiswedel): created
 */
package org.knime.bigdata.fileformats.orc;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.TypeDescription.Category;
import org.knime.bigdata.fileformats.orc.types.OrcBinaryTypeFactory;
import org.knime.bigdata.fileformats.orc.types.OrcBooleanTypeFactory;
import org.knime.bigdata.fileformats.orc.types.OrcDoubleTypeFactory;
import org.knime.bigdata.fileformats.orc.types.OrcIntTypeFactory;
import org.knime.bigdata.fileformats.orc.types.OrcListTypeFactory;
import org.knime.bigdata.fileformats.orc.types.OrcListTypeFactory.OrcListType;
import org.knime.bigdata.fileformats.orc.types.OrcLongTypeFactory;
import org.knime.bigdata.fileformats.orc.types.OrcStringTypeFactory;
import org.knime.bigdata.fileformats.orc.types.OrcTimestampTypeFactory;
import org.knime.bigdata.fileformats.orc.types.OrcType;
import org.knime.bigdata.fileformats.orc.types.OrcTypeFactory;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.blob.BinaryObjectDataCell;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;

/**
 * Table Store Format for ORC.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public final class OrcTableStoreFormat {

    private OrcTableStoreFormat() {
        throw new IllegalStateException("Utility class");
    }

    private static final Map<DataType, OrcTypeFactory<?>> TYPE_FACTORIES = new HashMap<>();

    private static final Map<Category, DataType> TYPEDESC_TYPE = new EnumMap<>(Category.class);

    static {
        register(new OrcStringTypeFactory());
        register(new OrcDoubleTypeFactory());
        register(new OrcIntTypeFactory());
        register(new OrcLongTypeFactory());
        register(new OrcBinaryTypeFactory());
        register(new OrcBooleanTypeFactory());
        register(new OrcBinaryTypeFactory());
        register(new OrcTimestampTypeFactory());

        TYPEDESC_TYPE.put(Category.INT, IntCell.TYPE);
        TYPEDESC_TYPE.put(Category.DOUBLE, DoubleCell.TYPE);
        TYPEDESC_TYPE.put(Category.LONG, LongCell.TYPE);
        TYPEDESC_TYPE.put(Category.STRING, StringCell.TYPE);
        TYPEDESC_TYPE.put(Category.BINARY, BinaryObjectDataCell.TYPE);
        TYPEDESC_TYPE.put(Category.BOOLEAN, BooleanCell.TYPE);
        TYPEDESC_TYPE.put(Category.BYTE, BinaryObjectDataCell.TYPE);
        TYPEDESC_TYPE.put(Category.CHAR, StringCell.TYPE);
        TYPEDESC_TYPE.put(Category.TIMESTAMP, LocalDateTimeCellFactory.TYPE);
        TYPEDESC_TYPE.put(Category.SHORT, IntCell.TYPE);
        TYPEDESC_TYPE.put(Category.FLOAT, DoubleCell.TYPE);
        TYPEDESC_TYPE.put(Category.DATE, LongCell.TYPE);
        TYPEDESC_TYPE.put(Category.DECIMAL, BinaryObjectDataCell.TYPE);
        TYPEDESC_TYPE.put(Category.VARCHAR, StringCell.TYPE);
        TYPEDESC_TYPE.put(Category.MAP, BinaryObjectDataCell.TYPE);
        TYPEDESC_TYPE.put(Category.STRUCT, BinaryObjectDataCell.TYPE);
        TYPEDESC_TYPE.put(Category.UNION, BinaryObjectDataCell.TYPE);
    }

    /**
     * Registers a OrcTypeFactory.
     *
     * @param fac the factory to register
     */
    public static synchronized <T extends OrcType<?>> void register(final OrcTypeFactory<T> fac) {
        TYPE_FACTORIES.putIfAbsent(fac.type(), fac);
    }

    /**
     * Creates a OrcType for the given {@link DataType}.
     *
     * @param type the data type
     * @return The corresponding OrcType for the type
     */
    @SuppressWarnings("unchecked")
    public static final <T extends OrcType<?>> T createOrcType(final DataType type) {
        if (type.isCollectionType()) {

            final OrcTypeFactory<?> orcTypeFactory = TYPE_FACTORIES.get(type.getCollectionElementType());
            if (orcTypeFactory == null) {
                throw new BigDataFileFormatException(
                        String.format("List of type %s not supported yet.", type.getCollectionElementType()));
            }
            final OrcType<ColumnVector> subtype = (OrcType<ColumnVector>) orcTypeFactory.create();
            final OrcListType orcType = OrcListTypeFactory.create(subtype);
            return (T) orcType;
        }
        if (TYPE_FACTORIES.get(type) == null) {
            throw new BigDataFileFormatException(String.format("Type %s not supported yet", type.getName()));
        }
        return (T) TYPE_FACTORIES.get(type).create();
    }

    /**
     * creates a OrcType for the given {@link Category}
     *
     * @param category the ORC {@link Category}
     * @return The corresponding DataType for the catergory
     */
    public static final DataType getDataType(Category category) {
        final DataType dataType = TYPEDESC_TYPE.get(category);
        if (dataType == null) {
            throw new BigDataFileFormatException(String.format("Type %s not supported yet", category.getName()));
        }
        return dataType;
    }

    /**
     * Returns an array of all unsupported types for a given table spec
     *
     * @param spec the table spec to check
     * @return the array containing the unsupported types
     */
    public static String[] getUnsupportedTypes(final DataTableSpec spec) {
        final List<String> unsupported = spec.stream().map(DataColumnSpec::getType).filter(t -> !t.isCollectionType())
                .filter(t -> !TYPE_FACTORIES.containsKey(t)).map(DataType::getName).collect(Collectors.toList());
        final List<String> listsunsupported = spec.stream().map(DataColumnSpec::getType)
                .filter(DataType::isCollectionType)
                .filter(t -> !TYPE_FACTORIES.containsKey(t.getCollectionElementType())).map(DataType::toString)
                .collect(Collectors.toList());
        unsupported.addAll(listsunsupported);
        return unsupported.toArray(new String[unsupported.size()]);
    }

    /**
     * Returns an array of all unsupported types for a given table spec
     *
     * @param spec the table spec to check
     * @return the array containing the unsupported types
     */
    public static boolean accepts(final DataTableSpec spec) {
        final boolean listsSupported = spec.stream().map(DataColumnSpec::getType).filter(DataType::isCollectionType)
                .allMatch(t -> TYPE_FACTORIES.containsKey(t.getCollectionElementType()));
        final boolean otherSupported = spec.stream().map(DataColumnSpec::getType).filter(t -> !t.isCollectionType())
                .allMatch(TYPE_FACTORIES::containsKey);
        return listsSupported && otherSupported;
    }

}
