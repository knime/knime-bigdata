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
 *   Sep 11, 2018 (Mareike Höger): created
 */

package org.knime.bigdata.fileformats.orc.datatype.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.knime.core.data.DataType;
import org.knime.core.data.blob.BinaryObjectDataCell;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.convert.ConverterFactory;
import org.knime.core.data.convert.datacell.JavaToDataCellConverterFactory;
import org.knime.core.data.convert.datacell.JavaToDataCellConverterRegistry;
import org.knime.core.data.convert.map.CellValueConsumerFactory;
import org.knime.core.data.convert.map.CellValueProducerFactory;
import org.knime.core.data.convert.map.ConsumerRegistry;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.data.convert.map.ProducerRegistry;
import org.knime.core.data.convert.map.ProductionPath;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.time.localdate.LocalDateCellFactory;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.Pair;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.datatype.mapping.DataTypeMappingRegistry;

/**
 * ORC Type Mapping Registry
 * 
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 */
public class ORCTypeMappingRegistry extends DataTypeMappingRegistry<TypeDescription, ORCSource, ORCDestination> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ORCTypeMappingRegistry.class);

    private static final ORCTypeMappingRegistry INSTANCE = new ORCTypeMappingRegistry();

    /**
     * @return a singleton instance of the registry
     */
    public static ORCTypeMappingRegistry getInstance() {
        return INSTANCE;
    }

    private final Collection<CellValueConsumerFactory<ORCDestination, ?, TypeDescription, ?>> m_consumerFactories;

    private final ConsumerRegistry<TypeDescription, ORCDestination> m_consumerRegistry;

    private final Collection<JavaToDataCellConverterFactory<?>> m_converterFactories;

    private final Map<Pair<DataType, TypeDescription>, ConsumptionPath> m_defaultConsumptionPaths;

    private final Map<Pair<TypeDescription, DataType>, ProductionPath> m_defaultProductionPaths;

    private final Collection<TypeDescription> m_externalSourceTypes;

    private final ProducerRegistry<TypeDescription, ORCSource> m_producerRegistry;

    private volatile Collection<DataType> m_knimeDestinationTypes;

    private ORCTypeMappingRegistry() {

        m_consumerRegistry = MappingFramework.forDestinationType(ORCDestination.class);
        m_producerRegistry = MappingFramework.forSourceType(ORCSource.class);
        // External source types
        m_externalSourceTypes = m_producerRegistry.getAllSourceTypes();
        // KNIME source types are lazily prepared. See getKnimeSourceTypes()
        // External destination type consumer factories
        m_consumerFactories = Collections.unmodifiableCollection(m_consumerRegistry.getAllConverterFactories());
        // KNIME destination type converter factories
        final JavaToDataCellConverterRegistry javaToKnimeTypeConverterRegistry = JavaToDataCellConverterRegistry
                .getInstance();
        m_converterFactories = Collections.unmodifiableCollection(
                m_producerRegistry.getAllConverterFactories().stream().map(CellValueProducerFactory::getDestinationType)
                .map(javaToKnimeTypeConverterRegistry::getFactoriesForSourceType).flatMap(Collection::stream)
                .collect(Collectors.toCollection(LinkedHashSet::new)));
        // Default consumption paths
        final Map<Pair<DataType, TypeDescription>, ConsumptionPath> defaultConsumptionPaths = new LinkedHashMap<>();
        addConsumptionPath(m_consumerRegistry, defaultConsumptionPaths, StringCell.TYPE,
                TypeDescription.createString());
        addConsumptionPath(m_consumerRegistry, defaultConsumptionPaths, DoubleCell.TYPE,
                TypeDescription.createDouble());
        addConsumptionPath(m_consumerRegistry, defaultConsumptionPaths, LongCell.TYPE, TypeDescription.createLong());
        addConsumptionPath(m_consumerRegistry, defaultConsumptionPaths, LocalDateTimeCellFactory.TYPE,
                TypeDescription.createTimestamp());
        addConsumptionPath(m_consumerRegistry, defaultConsumptionPaths, LocalDateCellFactory.TYPE,
                TypeDescription.createDate());
        addConsumptionPath(m_consumerRegistry, defaultConsumptionPaths, BooleanCell.TYPE,
                TypeDescription.createBoolean());
        addConsumptionPath(m_consumerRegistry, defaultConsumptionPaths, IntCell.TYPE, TypeDescription.createInt());
        addConsumptionPath(m_consumerRegistry, defaultConsumptionPaths, BinaryObjectDataCell.TYPE,
                TypeDescription.createBinary());

        m_defaultConsumptionPaths = Collections.unmodifiableMap(defaultConsumptionPaths);

        // Default production paths
        final Map<Pair<TypeDescription, DataType>, ProductionPath> defaultProductionPaths = new LinkedHashMap<>();
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createBoolean(),
                BooleanCell.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createByte(), IntCell.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createShort(), IntCell.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createInt(), IntCell.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createLong(), LongCell.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createFloat(), DoubleCell.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createDouble(), DoubleCell.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createDate(),
                LocalDateCellFactory.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createTimestamp(),
                LocalDateTimeCellFactory.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createBinary(),
                BinaryObjectDataCell.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createDecimal(), StringCell.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createVarchar(), StringCell.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createChar(), StringCell.TYPE);
        addProductionPath(m_producerRegistry, defaultProductionPaths, TypeDescription.createString(), StringCell.TYPE);
        m_defaultProductionPaths = Collections.unmodifiableMap(defaultProductionPaths);

        final DataTypeMappingConfiguration<TypeDescription> configuration = createMappingConfiguration(
                DataTypeMappingDirection.EXTERNAL_TO_KNIME);
        m_defaultProductionPaths.values().forEach(configuration::addRule);
        final DataType stringDataType = StringCell.TYPE;
        for (final Category orcType : Category.values()) {
            findPath(m_producerRegistry, new TypeDescription(orcType), stringDataType)
            .ifPresent(configuration::addRule);
        }

    }

    private void addConsumptionPath(final ConsumerRegistry<TypeDescription, ORCDestination> consumerRegistry,
            final Map<Pair<DataType, TypeDescription>, ConsumptionPath> defaultConsumptionPaths,
            final DataType knimeType, final TypeDescription externalType) {
        final Optional<ConsumptionPath> path = findPath(consumerRegistry, knimeType, externalType);
        if (path.isPresent()) {
            defaultConsumptionPaths.put(new Pair<>(knimeType, externalType), path.get());
        } else {
            LOGGER.error("Default consumption path is not available: " + knimeType + " -> " + externalType);
        }
        final DataType knimelisttype = ListCell.getCollectionType(knimeType);
        final TypeDescription listORCType = TypeDescription.createList(externalType);
        final Optional<ConsumptionPath> listpath = findPath(consumerRegistry, knimelisttype, listORCType);
        if (listpath.isPresent()) {
            defaultConsumptionPaths.put(new Pair<>(knimelisttype, listORCType), listpath.get());
        } else {
            LOGGER.error("Default consumption path is not available: " + knimelisttype + " -> " + listORCType);
        }

    }

    private void addProductionPath(final ProducerRegistry<TypeDescription, ORCSource> producerRegistry,
            final Map<Pair<TypeDescription, DataType>, ProductionPath> defaultProductionPaths,
            final TypeDescription externalType, final DataType knimeType) {
        final Optional<ProductionPath> path = findPath(producerRegistry, externalType, knimeType);
        if (path.isPresent()) {
            defaultProductionPaths.put(new Pair<>(externalType, knimeType), path.get());
        } else {
            LOGGER.error("Default production path is not available: " + externalType + " -> " + knimeType);
        }
        final DataType knimelisttype = ListCell.getCollectionType(knimeType);
        final TypeDescription listORCType = TypeDescription.createList(externalType);
        final Optional<ProductionPath> listpath = findPath(producerRegistry, listORCType, knimelisttype);
        if (listpath.isPresent()) {
            defaultProductionPaths.put(new Pair<>(listORCType, knimelisttype), listpath.get());
        } else {
            LOGGER.error("Default production path is not available: " + listORCType + " -> " + knimelisttype);
        }

    }

    @Override
    public ConsumerRegistry<TypeDescription, ORCDestination> getConsumerRegistry() {
        return m_consumerRegistry;
    }

    @Override
    public Collection<ConsumptionPath> getConsumptionPathsFor(DataType knimeType) {
        final List<ConsumptionPath> consumptionPaths = m_consumerRegistry.getAvailableConsumptionPaths(knimeType);
        final Set<ConsumptionPath> uniqueConsumptionPaths = new LinkedHashSet<>(consumptionPaths);
        return Collections.unmodifiableCollection(uniqueConsumptionPaths);
    }

    @Override
    public Collection<? extends ConverterFactory<Class<?>, TypeDescription>> getExternalMappings() {
        return Collections.unmodifiableCollection(m_consumerFactories);
    }

    @Override
    public Collection<TypeDescription> getExternalSourceTypes() {
        return Collections.unmodifiableCollection(m_externalSourceTypes);
    }

    @Override
    public Collection<? extends ConverterFactory<Class<?>, DataType>> getKnimeMappings() {
        return Collections.unmodifiableCollection(m_converterFactories);
    }

    @Override
    public Collection<DataType> getKnimeSourceTypes() {
        Collection<DataType> knimeDestinationTypes = m_knimeDestinationTypes;
        if (knimeDestinationTypes == null) {
            synchronized (this) {
                knimeDestinationTypes = m_knimeDestinationTypes;
                if (knimeDestinationTypes == null) {
                    final Set<DataType> uniqueKnimeDestinationTypes = new HashSet<>();
                    m_externalSourceTypes.stream().map(this::getProductionPathsFor).flatMap(Collection::stream)
                    .map(productionPath -> productionPath.m_converterFactory)
                    .map(ConverterFactory::getDestinationType).forEach(uniqueKnimeDestinationTypes::add);
                    final List<DataType> destinationTypes = new ArrayList<>(uniqueKnimeDestinationTypes);
                    Collections.sort(destinationTypes, Comparator.comparing(DataType::getName));
                    knimeDestinationTypes = Collections.unmodifiableCollection(destinationTypes);
                    m_knimeDestinationTypes = knimeDestinationTypes;
                }
            }
        }
        return knimeDestinationTypes;
    }

    @Override
    public ProducerRegistry<TypeDescription, ORCSource> getProducerRegistry() {
        return m_producerRegistry;
    }

    @Override
    public Collection<ProductionPath> getProductionPathsFor(TypeDescription externalType) {
        final List<ProductionPath> productionPaths = m_producerRegistry.getAvailableProductionPaths(externalType);
        final Set<ProductionPath> uniqueProductionPaths = new LinkedHashSet<>(productionPaths);
        return Collections.unmodifiableCollection(uniqueProductionPaths);
    }

    @Override
    public DataTypeMappingConfiguration<TypeDescription> newDefaultExternalToKnimeMappingConfiguration() {
        final DataTypeMappingConfiguration<TypeDescription> configuration = createMappingConfiguration(
                DataTypeMappingDirection.EXTERNAL_TO_KNIME);
        m_defaultProductionPaths.values().forEach(configuration::addRule);
        return configuration;
    }

    @Override
    public DataTypeMappingConfiguration<TypeDescription> newDefaultKnimeToExternalMappingConfiguration() {
        final DataTypeMappingConfiguration<TypeDescription> configuration = createMappingConfiguration(
                DataTypeMappingDirection.KNIME_TO_EXTERNAL);
        for (final Map.Entry<Pair<DataType, TypeDescription>, ConsumptionPath> entry : m_defaultConsumptionPaths
                .entrySet()) {
            configuration.addRule(entry.getKey().getFirst(), entry.getValue());
        }
        return configuration;
    }

}
