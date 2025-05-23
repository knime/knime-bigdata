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
package org.knime.bigdata.delta.nodes.reader.mapper;

import static org.knime.filehandling.core.node.table.reader.type.hierarchy.TypeTester.createTypeTester;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.knime.bigdata.delta.nodes.reader.DeltaTableReaderNodeSettings;
import org.knime.bigdata.delta.nodes.reader.framework.DeltaTableBooleanValue;
import org.knime.bigdata.delta.nodes.reader.framework.DeltaTableDoubleValue;
import org.knime.bigdata.delta.nodes.reader.framework.DeltaTableIntegerValue;
import org.knime.bigdata.delta.nodes.reader.framework.DeltaTableLongValue;
import org.knime.bigdata.delta.nodes.reader.framework.DeltaTableValue;
import org.knime.core.data.DataType;
import org.knime.core.data.blob.BinaryObjectDataCell;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.data.convert.map.ProducerRegistry;
import org.knime.core.data.convert.map.ProductionPath;
import org.knime.core.data.convert.map.SimpleCellValueProducerFactory;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.filehandling.core.node.table.reader.HierarchyAwareProductionPathProvider;
import org.knime.filehandling.core.node.table.reader.ProductionPathProvider;
import org.knime.filehandling.core.node.table.reader.ReadAdapter;
import org.knime.filehandling.core.node.table.reader.ReadAdapter.ReadAdapterParams;
import org.knime.filehandling.core.node.table.reader.ReadAdapterFactory;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TreeTypeHierarchy;
import org.knime.filehandling.core.node.table.reader.util.MultiTableUtils;

/**
 * Factory for {@link DeltaTableReadAdapter}.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public enum DeltaTableReadAdapterFactory implements ReadAdapterFactory<DataType, DeltaTableValue> {
    /**
     * The singleton instance.
     */
    INSTANCE;

    private static final ProducerRegistry<DataType, DeltaTableReadAdapter> PRODUCER_REGISTRY =
        initializeProducerRegistry();

    /**
     * The type hierarchy of the Tableau Reader.
     */
    public static final TreeTypeHierarchy<DataType,DataType> TYPE_HIERARCHY =
            createPrimitiveHierarchy().createTypeFocusedHierarchy();


    private static ProducerRegistry<DataType, DeltaTableReadAdapter> initializeProducerRegistry() {
        final ProducerRegistry<DataType, DeltaTableReadAdapter> registry = MappingFramework
                .forSourceType(DeltaTableReadAdapter.class);
        registry.register(new SimpleCellValueProducerFactory<>(BooleanCell.TYPE, Boolean.class,
            DeltaTableReadAdapterFactory::readBooleanFromSource));
        registry.register(new SimpleCellValueProducerFactory<>(IntCell.TYPE, Integer.class,
            DeltaTableReadAdapterFactory::readIntFromSource));
        registry.register(new SimpleCellValueProducerFactory<>(LongCell.TYPE, Long.class,
            DeltaTableReadAdapterFactory::readLongFromSource));
        registry.register(new SimpleCellValueProducerFactory<>(DoubleCell.TYPE, Double.class,
            DeltaTableReadAdapterFactory::readDoubleFromSource));
        registry.register(new SimpleCellValueProducerFactory<>(StringCell.TYPE, String.class,
            DeltaTableReadAdapterFactory::readStringFromSource));

        //        registry.register(new SupplierCellValueProducerFactory<>(BooleanCell.TYPE, Boolean.class,
        //                ObjectToBooleanCellValueProducer::new));
        //        registry.register(
        //                new SupplierCellValueProducerFactory<>(IntCell.TYPE, Integer.class, NumberToIntCellValueProducer::new));
        //        registry.register(new SupplierCellValueProducerFactory<>(DoubleCell.TYPE, Double.class,
        //                NumberToDoubleCellValueProducer::new));
        //        registry.register(
        //                new SupplierCellValueProducerFactory<>(LongCell.TYPE, Long.class, NumberToLongCellValueProducer::new));
        //        registry.register(new SimpleCellValueProducerFactory<>(LocalDateTimeCellFactory.TYPE, LocalDateTime.class,
        //                TableauHyperReadAdapterFactory::readLocalDateTimeFromSource));
        //        registry.register(new SimpleCellValueProducerFactory<>(LocalDateCellFactory.TYPE, LocalDate.class,
        //                TableauHyperReadAdapterFactory::readLocalDateFromSource));
        //        registry.register(new SimpleCellValueProducerFactory<>(ZonedDateTimeCellFactory.TYPE, ZonedDateTime.class,
        //                TableauHyperReadAdapterFactory::readZonedDateTimeFromSource));
        //        registry.register(new SimpleCellValueProducerFactory<>(LocalTimeCellFactory.TYPE, LocalTime.class,
        //                TableauHyperReadAdapterFactory::readLocalTimeFromSource));
        //        registry.register(new SimpleCellValueProducerFactory<>(JSONCell.TYPE, String.class,
        //                TableauHyperReadAdapterFactory::readStringFromSource));
        //        registry.register(new SimpleCellValueProducerFactory<>(BinaryObjectDataCell.TYPE, byte[].class,
        //                TableauHyperReadAdapterFactory::readBinaryFromSource));
        return registry;
    }

    private static boolean readBooleanFromSource(final DeltaTableReadAdapter source,
        final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params) {

        if (source.get(params) instanceof DeltaTableBooleanValue value) {
            return value.booleanValue();
        }

        throw new IllegalArgumentException("Invalid conversion");
    }

    private static int readIntFromSource(final DeltaTableReadAdapter source,
        final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params) {

        if (source.get(params) instanceof DeltaTableIntegerValue value) {
            return value.intValue();
        }

        throw new IllegalArgumentException("Invalid conversion");
    }

    private static long readLongFromSource(final DeltaTableReadAdapter source,
        final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params) {

        if (source.get(params) instanceof DeltaTableLongValue value) {
            return value.longValue();
        } else if (source.get(params) instanceof DeltaTableIntegerValue value) {
            return value.intValue();
        }

        throw new IllegalArgumentException("Invalid conversion");
    }

    private static double readDoubleFromSource(final DeltaTableReadAdapter source,
        final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params) {

        if (source.get(params) instanceof DeltaTableDoubleValue value) {
            return value.doubleValue();
        } else if (source.get(params) instanceof DeltaTableIntegerValue value) {
            return value.intValue();
        } else if (source.get(params) instanceof DeltaTableLongValue value) {
            return value.longValue();
        }

        throw new IllegalArgumentException("Invalid conversion");
    }

    private static String readStringFromSource(final DeltaTableReadAdapter source,
            final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params) {
        return source.get(params).stringValue();
    }

    //    private static String readString(final Object val) {
    //        if (val == null) {
    //            return null;
    //        }
    //        if (val instanceof Object[] array) {
    //            return Arrays.stream(array).map(DeltaTableReadAdapterFactory::readString)
    //                    .collect(Collectors.joining(",", "[", "]"));
    //        } else {
    //            return val.toString();
    //        }
    //    }
    //
    //    private static LocalDate readLocalDateFromSource(final DeltaTableReadAdapter source,
    //            final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params) {
    //        return (LocalDate) source.get(params);
    //    }
    //
    //    private static LocalTime readLocalTimeFromSource(final DeltaTableReadAdapter source,
    //            final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params) {
    //        return (LocalTime) source.get(params);
    //    }
    //
    //    private static byte[] readBinaryFromSource(final DeltaTableReadAdapter source,
    //            final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params) {
    //        return (byte[]) source.get(params);
    //    }
    //
    //    private static ZonedDateTime readZonedDateTimeFromSource(final DeltaTableReadAdapter source,
    //        final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params) {
    //
    //        final var obj = source.get(params);
    //        if (obj == null) {
    //            return null;
    //        }
    //
    //        if (obj instanceof OffsetDateTime offsetObj) {
    //            return offsetObj.toZonedDateTime();
    //        } else if (obj instanceof ZonedDateTime zonedObj) {
    //            return zonedObj;
    //        } else {
    //            throw new IllegalArgumentException("Value of type %s cannot be read as Zoned Date&Time"//
    //                .formatted(obj.getClass().getSimpleName()));
    //        }
    //    }
    //
    //    private static LocalDateTime readLocalDateTimeFromSource(final DeltaTableReadAdapter source,
    //            final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params) {
    //        final var val = source.get(params);
    //        if (val == null) {
    //            return null;
    //        }
    //        if (val instanceof LocalDate date) {
    //            return date.atStartOfDay();
    //        } else {
    //            return (LocalDateTime) val;
    //        }
    //    }
    //
    //    private abstract static class AbstractReadAdapterToPrimitiveCellValueProducer<S extends ReadAdapter<?, ?>, T>
    //            implements PrimitiveCellValueProducer<S, T, ReadAdapterParams<S, DeltaTableReaderNodeSettings>> {
    //
    //        @Override
    //        public final boolean producesMissingCellValue(final S source,
    //                final ReadAdapterParams<S, DeltaTableReaderNodeSettings> params) throws MappingException {
    //            return source.get(params) == null;
    //        }
    //    }
    //
    //    private static class ObjectToBooleanCellValueProducer
    //            extends AbstractReadAdapterToPrimitiveCellValueProducer<DeltaTableReadAdapter, Boolean>
    //            implements BooleanCellValueProducer<DeltaTableReadAdapter,
    //                        ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings>> {
    //
    //        @Override
    //        public boolean produceBooleanCellValue(final DeltaTableReadAdapter source,
    //                final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params)
    //                throws MappingException {
    //            return source.get(params).booleanValue();
    //        }
    //    }
    //
    //    private static class NumberToIntCellValueProducer
    //            extends AbstractReadAdapterToPrimitiveCellValueProducer<DeltaTableReadAdapter, Integer>
    //            implements IntCellValueProducer<DeltaTableReadAdapter,
    //                        ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings>> {
    //
    //        @Override
    //        public int produceIntCellValue(final DeltaTableReadAdapter source,
    //                final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params)
    //                throws MappingException {
    //            return source.get(params).intValue();
    //        }
    //    }
    //
    //    private static class NumberToDoubleCellValueProducer
    //            extends AbstractReadAdapterToPrimitiveCellValueProducer<DeltaTableReadAdapter, Double>
    //            implements DoubleCellValueProducer<DeltaTableReadAdapter,
    //                        ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings>> {
    //
    //        @Override
    //        public double produceDoubleCellValue(final DeltaTableReadAdapter source,
    //                final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params)
    //                throws MappingException {
    //            return source.get(params).doubleValue();
    //        }
    //    }
    //
    //    private static class NumberToLongCellValueProducer
    //            extends AbstractReadAdapterToPrimitiveCellValueProducer<DeltaTableReadAdapter, Long>
    //            implements LongCellValueProducer<DeltaTableReadAdapter,
    //                        ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings>> {
    //
    //        @Override
    //        public long produceLongCellValue(final DeltaTableReadAdapter source,
    //                final ReadAdapterParams<DeltaTableReadAdapter, DeltaTableReaderNodeSettings> params)
    //                throws MappingException {
    //            return source.get(params).longValue();
    //        }
    //    }

    @Override
    public ReadAdapter<DataType, DeltaTableValue> createReadAdapter() {
        return new DeltaTableReadAdapter();
    }

    @Override
    public ProducerRegistry<DataType, DeltaTableReadAdapter> getProducerRegistry() {
        return PRODUCER_REGISTRY;
    }

    @Override
    public DataType getDefaultType(final DataType type) {
        return type;
    }

    /**
     * @return a {@link HierarchyAwareProductionPathProvider} which does not map
     * Binary to String.
     */
    public ProductionPathProvider<DataType> createProductionPathProvider() {
        final var reachableDataTypes = new HashSet<>(
                MultiTableUtils.extractReachableKnimeTypes(PRODUCER_REGISTRY));
        return new BinaryFilteredProductionPathProvider<>(new HierarchyAwareProductionPathProvider<>(
                getProducerRegistry(), TYPE_HIERARCHY, this::getDefaultType,
                DeltaTableReadAdapterFactory::isValidPathFor, reachableDataTypes));
    }

    private static boolean isValidPathFor(final DataType type, final ProductionPath path) {
        // Tableau provides JSON as a string but that would allow JSON data to be mapped to all
        // types compatible with String which is likely to fail
        //        if (type == JSONCell.TYPE) {
        //            final var knimeType = path.getDestinationType();
        //            return JSONCell.TYPE.isCompatible(knimeType.getPreferredValueClass());
        //        } else {
            return true;
        //        }
    }

    static TreeTypeHierarchy<DataType, Object> createPrimitiveHierarchy() {
        return TreeTypeHierarchy.builder(createTypeTester(StringCell.TYPE, o -> true))//
            //                .addType(StringCell.TYPE, createTypeTester(JSONCell.TYPE, String.class::isInstance))//
                .addType(StringCell.TYPE, createTypeTester(DoubleCell.TYPE, Double.class::isInstance))//
                .addType(DoubleCell.TYPE, createTypeTester(LongCell.TYPE, Long.class::isInstance))//
                .addType(LongCell.TYPE, createTypeTester(IntCell.TYPE, Integer.class::isInstance))//
                .addType(StringCell.TYPE, createTypeTester(BooleanCell.TYPE, Boolean.class::isInstance))//
            //                .addType(StringCell.TYPE, createTypeTester(LocalTimeCellFactory.TYPE, LocalTime.class::isInstance))//
            //                .addType(StringCell.TYPE,  createTypeTester(LocalDateTimeCellFactory.TYPE, LocalDate.class::isInstance))
            //                .addType(LocalDateTimeCellFactory.TYPE, createTypeTester(LocalDateCellFactory.TYPE, LocalDate.class::isInstance))// NOSONAR
            //                .addType(StringCell.TYPE,  createTypeTester(ZonedDateTimeCellFactory.TYPE, ZonedDateTime.class::isInstance))// NOSONAR
                .addType(StringCell.TYPE,  createTypeTester(BinaryObjectDataCell.TYPE, byte[].class::isInstance))//
                .build();
    }

    /**
     * {@link ProductionPathProvider} that filters out any paths from Binary to other types than binary,
     * i.e. Binary → String, because Binary has no string representation.
     * As that path is always present it cannot be filtered by {@link DeltaTableReadAdapterFactory#isValidPathFor(DataType, ProductionPath)}
     * and has to be filtered using a wrapper.
     *
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    private static class BinaryFilteredProductionPathProvider<T> implements ProductionPathProvider<T> {

        private final ProductionPathProvider<T> m_inner;

        BinaryFilteredProductionPathProvider(final ProductionPathProvider<T> provider) {
            m_inner = provider;
        }

        @Override
        public ProductionPath getDefaultProductionPath(final T externalType) {
            return m_inner.getDefaultProductionPath(externalType);
        }

        @Override
        public List<ProductionPath> getAvailableProductionPaths(final T externalType) {
            final var result = m_inner.getAvailableProductionPaths(externalType);
            if (externalType == BinaryObjectDataCell.TYPE) {
                // only allow Binary Object to map to BinaryObject (without default to string)
                return result.stream()
                        .filter(p -> p.getDestinationType() == BinaryObjectDataCell.TYPE)
                        .collect(Collectors.toList()); // NOSONAR: result is also mutable
            } else {
                return result;
            }
        }

        @Override
        public Set<DataType> getAvailableDataTypes() {
            return m_inner.getAvailableDataTypes();
        }
    }
}
