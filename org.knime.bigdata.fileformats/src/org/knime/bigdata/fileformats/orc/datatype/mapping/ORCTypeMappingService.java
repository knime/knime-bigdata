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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.knime.core.data.DataType;
import org.knime.core.data.blob.BinaryObjectDataCell;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.convert.datacell.JavaToDataCellConverterRegistry;
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
import org.knime.core.data.time.duration.DurationCellFactory;
import org.knime.core.data.time.localdate.LocalDateCellFactory;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.core.data.time.localtime.LocalTimeCellFactory;
import org.knime.core.data.time.period.PeriodCellFactory;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.Pair;
import org.knime.datatype.mapping.AbstractDataTypeMappingService;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;

/**
 * ORC Type Mapping Registry
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 */
public class ORCTypeMappingService extends AbstractDataTypeMappingService<TypeDescription, ORCSource, ORCDestination> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ORCTypeMappingService.class);

    private static final ORCTypeMappingService INSTANCE = new ORCTypeMappingService();

    /**
     * @return a singleton instance of the registry
     */
    public static ORCTypeMappingService getInstance() {
        return INSTANCE;
    }

    private ORCTypeMappingService() {

        final ConsumerRegistry<TypeDescription, ORCDestination> consumerRegistry =
                MappingFramework.forDestinationType(ORCDestination.class);
        setConsumerRegistry(consumerRegistry);
        final ProducerRegistry<TypeDescription, ORCSource> producerRegistry =
                MappingFramework.forSourceType(ORCSource.class);
        setProducerRegistry(producerRegistry);
        // External source types
        setExternalSourceTypes(producerRegistry.getAllSourceTypes());
        // KNIME source types are lazily prepared. See getKnimeSourceTypes()
        // External destination type consumer factories
        setConsumerFactories(Collections.unmodifiableCollection(consumerRegistry.getAllConverterFactories()));
        // KNIME destination type converter factories
        final JavaToDataCellConverterRegistry javaToKnimeTypeConverterRegistry = JavaToDataCellConverterRegistry
                .getInstance();
        setConverterFactories(Collections.unmodifiableCollection(
                producerRegistry.getAllConverterFactories().stream().map(CellValueProducerFactory::getDestinationType)
                .map(javaToKnimeTypeConverterRegistry::getFactoriesForSourceType).flatMap(Collection::stream)
                .collect(Collectors.toCollection(LinkedHashSet::new))));
        // Default consumption paths
        final Map<Pair<DataType, TypeDescription>, ConsumptionPath> defaultConsumptionPaths = new LinkedHashMap<>();
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, StringCell.TYPE,
                TypeDescription.createString());
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, DoubleCell.TYPE,
                TypeDescription.createDouble());
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, LongCell.TYPE, TypeDescription.createLong());
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, LocalDateTimeCellFactory.TYPE,
                TypeDescription.createTimestamp());
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, LocalTimeCellFactory.TYPE,
                TypeDescription.createString());
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, LocalDateCellFactory.TYPE,
                TypeDescription.createDate());
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, ZonedDateTimeCellFactory.TYPE,
                TypeDescription.createString());
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, PeriodCellFactory.TYPE,
                TypeDescription.createString());
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, DurationCellFactory.TYPE,
                TypeDescription.createString());
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, BooleanCell.TYPE,
                TypeDescription.createBoolean());
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, IntCell.TYPE, TypeDescription.createInt());
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, BinaryObjectDataCell.TYPE,
                TypeDescription.createBinary());
        
        setDefaultConsumptionPaths(Collections.unmodifiableMap(defaultConsumptionPaths));

        // Default production paths
        final Map<Pair<TypeDescription, DataType>, ProductionPath> defaultProductionPaths = new LinkedHashMap<>();
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createBoolean(),
                BooleanCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createByte(), IntCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createShort(), IntCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createInt(), IntCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createLong(), LongCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createFloat(), DoubleCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createDouble(), DoubleCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createDate(),
                LocalDateCellFactory.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createTimestamp(),
                LocalDateTimeCellFactory.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createBinary(),
                BinaryObjectDataCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createDecimal(), StringCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createVarchar(), StringCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createChar(), StringCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, TypeDescription.createString(), StringCell.TYPE);
        setDefaultProductionPaths(Collections.unmodifiableMap(defaultProductionPaths));

        final DataTypeMappingConfiguration<TypeDescription> configuration = createMappingConfiguration(
                DataTypeMappingDirection.EXTERNAL_TO_KNIME);
        defaultProductionPaths.values().forEach(configuration::addRule);
        final DataType stringDataType = StringCell.TYPE;
        for (final Category orcType : Category.values()) {
            findPath(producerRegistry, new TypeDescription(orcType), stringDataType)
            .ifPresent(configuration::addRule);
        }

    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void addConsumptionPath(final ConsumerRegistry<TypeDescription, ORCDestination> consumerRegistry,
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

    /**
     * {@inheritDoc}
     */
    @Override
    protected void addProductionPath(final ProducerRegistry<TypeDescription, ORCSource> producerRegistry,
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
    public String convertExternalTypeToString(final TypeDescription externalType) {
        return externalType.toString();
    }

    @Override
    public TypeDescription convertStringToExternalType(final String string) {
        return TypeDescription.fromString(string);
    }

}
