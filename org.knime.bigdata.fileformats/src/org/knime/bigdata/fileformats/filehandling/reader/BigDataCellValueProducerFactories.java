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
 *   Nov 9, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader;

import java.util.function.Supplier;

import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.bigdata.fileformats.filehandling.reader.type.ForestTypeHierarchy;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.PrimitiveKnimeType;
import org.knime.core.data.convert.map.BooleanCellValueProducer;
import org.knime.core.data.convert.map.CellValueProducer;
import org.knime.core.data.convert.map.CellValueProducerFactory;
import org.knime.core.data.convert.map.DoubleCellValueProducer;
import org.knime.core.data.convert.map.IntCellValueProducer;
import org.knime.core.data.convert.map.LongCellValueProducer;
import org.knime.core.data.convert.map.MappingException;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.data.convert.map.PrimitiveCellValueProducer;
import org.knime.core.data.convert.map.ProducerRegistry;
import org.knime.filehandling.core.node.table.reader.ReadAdapter.ReadAdapterParams;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TreeTypeHierarchy;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TreeTypeHierarchy.TreeNode;

/**
 * Contains the various {@link CellValueProducerFactory CellValueProducerFactory} and {@link CellValueProducer}
 * implementations for mapping {@link BigDataCell}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class BigDataCellValueProducerFactories {

    private BigDataCellValueProducerFactories() {
        // This class is never instantiated
    }

    static ProducerRegistry<KnimeType, BigDataReadAdapter>
        createRegistry(final ForestTypeHierarchy<KnimeType, KnimeType> typeHierarchy) {
        final ProducerRegistry<KnimeType, BigDataReadAdapter> registry =
            MappingFramework.forSourceType(BigDataReadAdapter.class);
        for (TreeTypeHierarchy<KnimeType, KnimeType> tree : typeHierarchy) {
            fillRegistryRecursively(registry, tree.getRootNode());
        }
        return registry;
    }

    private static void fillRegistryRecursively(final ProducerRegistry<KnimeType, BigDataReadAdapter> registry,
        final TreeNode<KnimeType, KnimeType> node) {
        addProducerFactories(registry, node.getType());
        for (TreeNode<KnimeType, KnimeType> child : node.getChildren()) {
            fillRegistryRecursively(registry, child);
        }
    }

    private static void addProducerFactories(final ProducerRegistry<KnimeType, BigDataReadAdapter> registry,
        final KnimeType knimeType) {
        if (knimeType.isList()) {
            createListCellValueProducerFactory(registry, knimeType);
        } else {
            addPrimitiveProducerFactories(registry, knimeType.asPrimitiveType());
        }
    }

    private static void createListCellValueProducerFactory(
        final ProducerRegistry<KnimeType, BigDataReadAdapter> registry, final KnimeType knimeType) {
        for (Class<?> supportedJavaClass : knimeType.getSupportedJavaClasses()) {
            registry.register(new ArrayBigDataCellValueProducerFactory<>(supportedJavaClass, knimeType));
        }
    }

    private static void addPrimitiveProducerFactories(final ProducerRegistry<KnimeType, BigDataReadAdapter> registry, //NOSONAR
        final PrimitiveKnimeType primitiveKnimeType) {
        switch (primitiveKnimeType) {
            case BOOLEAN:
                registry.register(new BigDataProducerFactory<>(primitiveKnimeType, Boolean.class,
                    BooleanBigDataCellValueProducer::new));
                break;
            case DOUBLE:
                registry.register(new BigDataProducerFactory<>(primitiveKnimeType, Double.class,
                    DoubleBigDataCellValueProducer::new));
                break;
            case INTEGER:
                registry.register(
                    new BigDataProducerFactory<>(primitiveKnimeType, Integer.class, IntBigDataCellValueProducer::new));
                break;
            case LONG:
                registry.register(
                    new BigDataProducerFactory<>(primitiveKnimeType, Long.class, LongBigDataCellValueProducer::new));
                break;
            case STRING:
                registry.register(new BigDataProducerFactory<>(primitiveKnimeType, String.class,
                    StringBigDataCellValueProducer::new));
                break;
            case BINARY:
            case DATE:
            case TIME:
            case INSTANT_DATE_TIME:
            case LOCAL_DATE_TIME:
                for (final Class<?> supportedClass : primitiveKnimeType.getSupportedJavaClasses()) {
                    registry.register(createObjProducerFactory(primitiveKnimeType, supportedClass));
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported primitive type encountered: " + primitiveKnimeType);
        }
    }

    private static <J> BigDataProducerFactory<J, BigDataObjCellValueProducer<J>>
        createObjProducerFactory(final PrimitiveKnimeType primitiveKnimeType, final Class<J> objClass) {
        return new BigDataProducerFactory<>(primitiveKnimeType, objClass,
            () -> new BigDataObjCellValueProducer<>(objClass));
    }

    private static class BigDataProducerFactory<J, P extends CellValueProducer<BigDataReadAdapter, J, ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig>>>
        implements
        CellValueProducerFactory<BigDataReadAdapter, KnimeType, J, ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig>> {

        private final KnimeType m_sourceType;

        private final Class<?> m_destinationType;

        private final Supplier<P> m_producerSupplier;

        BigDataProducerFactory(final KnimeType sourceType, final Class<?> destinationType,
            final Supplier<P> producerSupplier) {
            m_sourceType = sourceType;
            m_producerSupplier = producerSupplier;
            m_destinationType = destinationType;
        }

        @Override
        public Class<?> getDestinationType() {
            return m_destinationType;
        }

        @Override
        public KnimeType getSourceType() {
            return m_sourceType;
        }

        @Override
        public String getIdentifier() {
            return m_sourceType + "->" + m_destinationType;
        }

        @Override
        public P create() {
            return m_producerSupplier.get();
        }

    }

    private abstract static class AbstractPrimitiveBigDataCellValueProducer<J> implements
        PrimitiveCellValueProducer<BigDataReadAdapter, J, ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig>> {

        @Override
        public boolean producesMissingCellValue(final BigDataReadAdapter source,
            final ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig> params) throws MappingException {
            final BigDataCell cell = source.get(params);
            // cell == null indicates that the value is virtual i.e. the respective column isn't contained in the current file
            // if cell.isSet() returns false, then the value was missing in the file
            return cell == null || cell.isNull();
        }
    }

    private static final class IntBigDataCellValueProducer extends AbstractPrimitiveBigDataCellValueProducer<Integer>
        implements
        IntCellValueProducer<BigDataReadAdapter, ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig>> {

        @Override
        public int produceIntCellValue(final BigDataReadAdapter source,
            final ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig> params) throws MappingException {
            return source.get(params).getInt();
        }

    }

    private static final class LongBigDataCellValueProducer extends AbstractPrimitiveBigDataCellValueProducer<Long>
        implements
        LongCellValueProducer<BigDataReadAdapter, ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig>> {

        @Override
        public long produceLongCellValue(final BigDataReadAdapter source,
            final ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig> params) throws MappingException {
            return source.get(params).getLong();
        }

    }

    private static final class DoubleBigDataCellValueProducer extends AbstractPrimitiveBigDataCellValueProducer<Double>
        implements
        DoubleCellValueProducer<BigDataReadAdapter, ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig>> {

        @Override
        public double produceDoubleCellValue(final BigDataReadAdapter source,
            final ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig> params) throws MappingException {
            return source.get(params).getDouble();
        }

    }

    private static final class BooleanBigDataCellValueProducer
        extends AbstractPrimitiveBigDataCellValueProducer<Boolean> implements
        BooleanCellValueProducer<BigDataReadAdapter, ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig>> {

        @Override
        public boolean produceBooleanCellValue(final BigDataReadAdapter source,
            final ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig> params) throws MappingException {
            return source.get(params).getBoolean();
        }

    }

    private static class StringBigDataCellValueProducer implements
        CellValueProducer<BigDataReadAdapter, String, ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig>> {

        @Override
        public String produceCellValue(final BigDataReadAdapter source,
            final ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig> params) throws MappingException {
            final BigDataCell cell = source.get(params);
            return (cell == null || cell.isNull()) ? null : cell.getString();
        }

    }

    private static class BigDataObjCellValueProducer<T> implements
        CellValueProducer<BigDataReadAdapter, T, ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig>> {

        private final Class<T> m_objClass;

        BigDataObjCellValueProducer(final Class<T> objClass) {
            m_objClass = objClass;
        }

        @Override
        public T produceCellValue(final BigDataReadAdapter source,
            final ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig> params) throws MappingException {
            final BigDataCell cell = source.get(params);
            return (cell == null || cell.isNull()) ? null : cell.getObj(m_objClass);
        }

    }

    private static final class ArrayBigDataCellValueProducerFactory<A> implements
        CellValueProducerFactory<BigDataReadAdapter, KnimeType, A, ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig>> {

        private final Class<A> m_arrayClass;

        private final KnimeType m_knimeType;

        ArrayBigDataCellValueProducerFactory(final Class<A> arrayClass, final KnimeType knimeType) {
            m_arrayClass = arrayClass;
            m_knimeType = knimeType;
        }

        @Override
        public Class<?> getDestinationType() {
            return m_arrayClass;
        }

        @Override
        public KnimeType getSourceType() {
            return m_knimeType;
        }

        @Override
        public String getIdentifier() {
            return m_knimeType + "->" + m_arrayClass.getSimpleName();
        }

        @Override
        public CellValueProducer<BigDataReadAdapter, A, ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig>>
            create() {
            return this::produceCellValue;
        }

        private A produceCellValue(final BigDataReadAdapter source,
            final ReadAdapterParams<BigDataReadAdapter, BigDataReaderConfig> params) {
            final BigDataCell cell = source.get(params);
            return (cell == null || cell.isNull()) ? null : cell.getObj(m_arrayClass);
        }

    }

}
