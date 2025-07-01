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
 *   2025-05-21 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.iceberg.nodes.reader.mapper;

import static org.knime.filehandling.core.node.table.reader.type.hierarchy.TypeTester.createTypeTester;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.function.Function;

import org.knime.bigdata.iceberg.nodes.reader.IcebergReaderConfig;
import org.knime.bigdata.iceberg.types.IcebergDataType;
import org.knime.bigdata.iceberg.types.IcebergListType;
import org.knime.bigdata.iceberg.types.IcebergPrimitiveType;
import org.knime.bigdata.iceberg.types.IcebergValue;
import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.data.convert.map.ProducerRegistry;
import org.knime.core.data.convert.map.SimpleCellValueProducerFactory;
import org.knime.filehandling.core.node.table.reader.HierarchyAwareProductionPathProvider;
import org.knime.filehandling.core.node.table.reader.ProductionPathProvider;
import org.knime.filehandling.core.node.table.reader.ReadAdapter;
import org.knime.filehandling.core.node.table.reader.ReadAdapter.ReadAdapterParams;
import org.knime.filehandling.core.node.table.reader.ReadAdapterFactory;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TreeTypeHierarchy;
import org.knime.filehandling.core.node.table.reader.util.MultiTableUtils;


/**
 * Factory for {@link IcebergReadAdapter}.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public enum IcebergReadAdapterFactory implements ReadAdapterFactory<IcebergDataType, IcebergValue> {

    /**
     * The singleton instance.
     */
    INSTANCE;

    private static final ProducerRegistry<IcebergDataType, IcebergReadAdapter> PRODUCER_REGISTRY =
        initializeProducerRegistry();

    /**
     * The type hierarchy of the Tableau Reader.
     */
    public static final TreeTypeHierarchy<IcebergDataType, IcebergDataType> TYPE_HIERARCHY =
            createTypeHierarchy().createTypeFocusedHierarchy();


    private static ProducerRegistry<IcebergDataType, IcebergReadAdapter> initializeProducerRegistry() {
        final ProducerRegistry<IcebergDataType, IcebergReadAdapter> registry = MappingFramework
                .forSourceType(IcebergReadAdapter.class);

        // primitive types
        registry.register(new SimpleValueProducerFactory<>(IcebergPrimitiveType.BINARY, InputStream.class,
            v -> v.getObj(InputStream.class)));

        registry.register(new SimpleValueProducerFactory<>(IcebergPrimitiveType.BOOLEAN, Boolean.class,
            IcebergValue::getBoolean));
        registry.register(new SimpleValueProducerFactory<>(IcebergPrimitiveType.INTEGER, Integer.class,
            IcebergValue::getInt));
        registry.register(new SimpleValueProducerFactory<>(IcebergPrimitiveType.LONG, Long.class,
            IcebergValue::getLong));
        registry.register(new SimpleValueProducerFactory<>(IcebergPrimitiveType.DOUBLE, Double.class,
            IcebergValue::getDouble));
        registry.register(new SimpleValueProducerFactory<>(IcebergPrimitiveType.STRING, String.class,
            IcebergValue::getString));

        // date and time types
        registry.register(new SimpleValueProducerFactory<>(IcebergPrimitiveType.DATE, LocalDate.class,
            v -> v.getObj(LocalDate.class)));
        registry.register(new SimpleValueProducerFactory<>(IcebergPrimitiveType.INSTANT_DATE_TIME,
            ZonedDateTime.class, v -> v.getObj(ZonedDateTime.class)));
        registry.register(new SimpleValueProducerFactory<>(IcebergPrimitiveType.LOCAL_DATE_TIME, LocalDateTime.class,
            v -> v.getObj(LocalDateTime.class)));

        // array types
        registry.register(new SimpleValueProducerFactory<>(IcebergListType.of(IcebergPrimitiveType.BOOLEAN),
            Boolean[].class, v -> v.getObj(Boolean[].class)));
        registry.register(new SimpleValueProducerFactory<>(IcebergListType.of(IcebergPrimitiveType.INTEGER),
            Integer[].class, v -> v.getObj(Integer[].class)));
        registry.register(new SimpleValueProducerFactory<>(IcebergListType.of(IcebergPrimitiveType.LONG),
            Long[].class, v -> v.getObj(Long[].class)));
        registry.register(new SimpleValueProducerFactory<>(IcebergListType.of(IcebergPrimitiveType.DOUBLE),
            Double[].class, v -> v.getObj(Double[].class)));
        registry.register(new SimpleValueProducerFactory<>(IcebergListType.of(IcebergPrimitiveType.STRING),
            String[].class, v -> v.getObj(String[].class)));

        // date and time array types
        registry.register(new SimpleValueProducerFactory<>(IcebergListType.of(IcebergPrimitiveType.DATE),
            LocalDate[].class, v -> v.getObj(LocalDate[].class)));
        registry
            .register(new SimpleValueProducerFactory<>(IcebergListType.of(IcebergPrimitiveType.INSTANT_DATE_TIME),
                ZonedDateTime[].class, v -> v.getObj(ZonedDateTime[].class)));
        registry
            .register(new SimpleValueProducerFactory<>(IcebergListType.of(IcebergPrimitiveType.LOCAL_DATE_TIME),
                LocalDateTime[].class, v -> v.getObj(LocalDateTime[].class)));

        return registry;
    }

    private static final class SimpleValueProducerFactory<T> extends SimpleCellValueProducerFactory //
        <IcebergReadAdapter, IcebergDataType, T, ReadAdapterParams<IcebergReadAdapter, IcebergReaderConfig>> { // NOSONAR

        SimpleValueProducerFactory(final IcebergDataType externalType, final Class<T> destType,
            final Function<IcebergValue, T> reader) {
            super(externalType, destType, (s, p) -> reader.apply(s.get(p)));
        }

    }

    @Override
    public ReadAdapter<IcebergDataType, IcebergValue> createReadAdapter() {
        return new IcebergReadAdapter();
    }

    @Override
    public ProducerRegistry<IcebergDataType, IcebergReadAdapter> getProducerRegistry() {
        return PRODUCER_REGISTRY;
    }

    @Override
    public DataType getDefaultType(final IcebergDataType type) {
        return type.getDefaultDataType();
    }

    /**
     * @return a {@link HierarchyAwareProductionPathProvider}
     */
    public ProductionPathProvider<IcebergDataType> createProductionPathProvider() {
        final var reachableDataTypes = new HashSet<>(
                MultiTableUtils.extractReachableKnimeTypes(PRODUCER_REGISTRY));
        return new HierarchyAwareProductionPathProvider<>(getProducerRegistry(), TYPE_HIERARCHY, this::getDefaultType,
            (t, p) -> true /* valid path */, reachableDataTypes);
    }

    static TreeTypeHierarchy<IcebergDataType, Object> createTypeHierarchy() {
        return TreeTypeHierarchy
            .<IcebergDataType, Object> builder(createTypeTester(IcebergPrimitiveType.STRING, o -> true)) //

            // primitive types
            .addType(IcebergPrimitiveType.STRING, //
                createTypeTester(IcebergPrimitiveType.BINARY, InputStream.class::isInstance)) //
            .addType(IcebergPrimitiveType.STRING, //
                createTypeTester(IcebergPrimitiveType.DOUBLE, Double.class::isInstance)) //
            .addType(IcebergPrimitiveType.DOUBLE, //
                createTypeTester(IcebergPrimitiveType.LONG, Long.class::isInstance)) //
            .addType(IcebergPrimitiveType.LONG, //
                createTypeTester(IcebergPrimitiveType.INTEGER, Integer.class::isInstance)) //
            .addType(IcebergPrimitiveType.STRING, //
                createTypeTester(IcebergPrimitiveType.BOOLEAN, Boolean.class::isInstance)) //

            // date and time types
            .addType(IcebergPrimitiveType.STRING, //
                createTypeTester(IcebergPrimitiveType.INSTANT_DATE_TIME, ZonedDateTime.class::isInstance)) //
            .addType(IcebergPrimitiveType.INSTANT_DATE_TIME, //
                createTypeTester(IcebergPrimitiveType.LOCAL_DATE_TIME, LocalDateTime.class::isInstance)) //
            .addType(IcebergPrimitiveType.LOCAL_DATE_TIME, //
                createTypeTester(IcebergPrimitiveType.DATE, LocalDate.class::isInstance)) //

            // array types
            .addType(IcebergPrimitiveType.STRING, //
                createTypeTester(IcebergListType.of(IcebergPrimitiveType.STRING), String[].class::isInstance))
            .addType(IcebergListType.of(IcebergPrimitiveType.STRING), //
                createTypeTester(IcebergListType.of(IcebergPrimitiveType.DOUBLE), Double[].class::isInstance))
            .addType(IcebergListType.of(IcebergPrimitiveType.DOUBLE), //
                createTypeTester(IcebergListType.of(IcebergPrimitiveType.LONG), Long[].class::isInstance))
            .addType(IcebergListType.of(IcebergPrimitiveType.LONG), //
                createTypeTester(IcebergListType.of(IcebergPrimitiveType.INTEGER), Integer[].class::isInstance))
            .addType(IcebergListType.of(IcebergPrimitiveType.STRING), //
                createTypeTester(IcebergListType.of(IcebergPrimitiveType.BOOLEAN), Boolean[].class::isInstance))

            // date and time array types
            .addType(IcebergListType.of(IcebergPrimitiveType.STRING), //
                createTypeTester(IcebergListType.of(IcebergPrimitiveType.INSTANT_DATE_TIME), //
                    ZonedDateTime[].class::isInstance)) //
            .addType(IcebergListType.of(IcebergPrimitiveType.INSTANT_DATE_TIME), //
                createTypeTester(IcebergListType.of(IcebergPrimitiveType.LOCAL_DATE_TIME), //
                    LocalDate[].class::isInstance)) //
            .addType(IcebergListType.of(IcebergPrimitiveType.LOCAL_DATE_TIME), //
                createTypeTester(IcebergListType.of(IcebergPrimitiveType.DATE), //
                    LocalDate[].class::isInstance)) //

            .build();
    }

}
