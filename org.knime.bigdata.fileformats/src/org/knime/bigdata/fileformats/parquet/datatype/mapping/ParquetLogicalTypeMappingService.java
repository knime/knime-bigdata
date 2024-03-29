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
 */

package org.knime.bigdata.fileformats.parquet.datatype.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
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
 * Type mapping registry for Parquet types
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class ParquetLogicalTypeMappingService
        extends AbstractDataTypeMappingService<ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ParquetLogicalTypeMappingService.class);

    private static final ParquetLogicalTypeMappingService INSTANCE = new ParquetLogicalTypeMappingService();

    /**
     * @return a singleton instance of the registry
     */
    public static ParquetLogicalTypeMappingService getInstance() {
        return INSTANCE;
    }

    /**
     * Creates a Type Mapping Registry for Parquet
     */
    private ParquetLogicalTypeMappingService() {
        final ConsumerRegistry<ParquetType, ParquetLogicalTypeDestination> consumerRegistry = MappingFramework
                .forDestinationType(ParquetLogicalTypeDestination.class);
        setConsumerRegistry(consumerRegistry);

        final ProducerRegistry<ParquetType, ParquetLogicalTypeSource> producerRegistry = MappingFramework
                .forSourceType(ParquetLogicalTypeSource.class);
        setProducerRegistry(producerRegistry);
        // External source types
        setExternalSourceTypes(producerRegistry.getAllSourceTypes());
        // KNIME source types are lazily prepared. See getKnimeSourceTypes()
        // External destination type consumer factories
        setConsumerFactories(Collections.unmodifiableCollection(consumerRegistry.getAllConverterFactories()));
        // KNIME destination type converter factories
        final var javaToKnimeTypeConverterRegistry = JavaToDataCellConverterRegistry.getInstance();
        setConverterFactories(Collections.unmodifiableCollection(
                producerRegistry.getAllConverterFactories().stream().map(CellValueProducerFactory::getDestinationType)
                        .map(javaToKnimeTypeConverterRegistry::getFactoriesForSourceType).flatMap(Collection::stream)
                        .collect(Collectors.toCollection(LinkedHashSet::new))));

        final Map<Pair<DataType, ParquetType>, ConsumptionPath> defaultConsumptionPaths = new LinkedHashMap<>();
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, StringCell.TYPE,
                new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, BinaryObjectDataCell.TYPE,
                new ParquetType(PrimitiveTypeName.BINARY));
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, BooleanCell.TYPE,
                new ParquetType(PrimitiveTypeName.BOOLEAN));
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, DoubleCell.TYPE,
                new ParquetType(PrimitiveTypeName.DOUBLE));
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, IntCell.TYPE,
                new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, true)));
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, LongCell.TYPE,
                new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, true)));
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, LocalDateCellFactory.TYPE,
            new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType()));
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, LocalTimeCellFactory.TYPE,
                new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.timeType(false, TimeUnit.MILLIS)));
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, LocalDateTimeCellFactory.TYPE,
                new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.MILLIS)));
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, ZonedDateTimeCellFactory.TYPE,
            new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS)));
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, DurationCellFactory.TYPE,
                new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
        addConsumptionPath(consumerRegistry, defaultConsumptionPaths, PeriodCellFactory.TYPE,
                new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));

        setDefaultConsumptionPaths(Collections.unmodifiableMap(defaultConsumptionPaths));


        // Default production paths
        final Map<Pair<ParquetType, DataType>, ProductionPath> defaultProductionPaths = new LinkedHashMap<>();
        addProductionPath(producerRegistry, defaultProductionPaths, new ParquetType(PrimitiveTypeName.BOOLEAN),
                BooleanCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
                new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.enumType()), StringCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
                new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.jsonType()), StringCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
                new ParquetType(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()), StringCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, new ParquetType(PrimitiveTypeName.BINARY),
                BinaryObjectDataCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, new ParquetType(PrimitiveTypeName.INT96),
                BinaryObjectDataCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
                new ParquetType(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY), BinaryObjectDataCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, new ParquetType(PrimitiveTypeName.DOUBLE),
                DoubleCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths, new ParquetType(PrimitiveTypeName.FLOAT),
                DoubleCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
                new ParquetType(PrimitiveTypeName.INT32), IntCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
                new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)), IntCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
                new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16, true)), IntCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
                new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, true)), IntCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
                new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType()), LocalDateCellFactory.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
            new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timeType(false, TimeUnit.NANOS)),
            LocalTimeCellFactory.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
            new ParquetType(PrimitiveTypeName.INT32, LogicalTypeAnnotation.timeType(false, TimeUnit.MILLIS)),
            LocalTimeCellFactory.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
            new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS)),
            LocalTimeCellFactory.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
                new ParquetType(PrimitiveTypeName.INT64), LongCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
                new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, true)), LongCell.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
            new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS)),
            LocalDateTimeCellFactory.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
            new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.MILLIS)),
            LocalDateTimeCellFactory.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
            new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS)),
            LocalDateTimeCellFactory.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
            new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS)),
            ZonedDateTimeCellFactory.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
            new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS)),
            ZonedDateTimeCellFactory.TYPE);
        addProductionPath(producerRegistry, defaultProductionPaths,
            new ParquetType(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS)),
            ZonedDateTimeCellFactory.TYPE);

        setDefaultProductionPaths(Collections.unmodifiableMap(defaultProductionPaths));
        final DataTypeMappingConfiguration<ParquetType> configuration = createMappingConfiguration(
                DataTypeMappingDirection.EXTERNAL_TO_KNIME);

        defaultProductionPaths.values().forEach(configuration::addRule);
    }

    @Override
    protected void addConsumptionPath(final ConsumerRegistry<ParquetType, ParquetLogicalTypeDestination> consumerRegistry,
            final Map<Pair<DataType, ParquetType>, ConsumptionPath> defaultConsumptionPaths, final DataType knimeType,
            final ParquetType externalType) {
        final Optional<ConsumptionPath> path = findPath(consumerRegistry, knimeType, externalType);
        if (path.isPresent()) {
            defaultConsumptionPaths.put(new Pair<>(knimeType, externalType), path.get());
        } else {
            LOGGER.error("Default consumption path is not available: " + knimeType + " -> " + externalType);
        }
        final DataType knimelisttype = ListCell.getCollectionType(knimeType);

        final var listType = ParquetType.createListType(externalType);

        final Optional<ConsumptionPath> listpath = findPath(consumerRegistry, knimelisttype, listType);
        if (listpath.isPresent()) {
            defaultConsumptionPaths.put(new Pair<>(knimelisttype, listType), listpath.get());
        } else {
            LOGGER.error("Default consumption path is not available: " + knimelisttype + " -> " + listType);
        }

    }

    @Override
    protected void addProductionPath(final ProducerRegistry<ParquetType, ParquetLogicalTypeSource> producerRegistry,
            final Map<Pair<ParquetType, DataType>, ProductionPath> defaultProductionPaths,
            final ParquetType externalType, final DataType knimeType) {
        final Optional<ProductionPath> path = findPath(producerRegistry, externalType, knimeType);
        if (path.isPresent()) {
            defaultProductionPaths.put(new Pair<>(externalType, knimeType), path.get());
        } else {
            LOGGER.error("Default production path is not available: " + externalType + " -> " + knimeType);
        }

        final DataType knimeListType = ListCell.getCollectionType(knimeType);

        final var listType = ParquetType.createListType(externalType);

        final Optional<ProductionPath> listpath = findPath(producerRegistry, listType, knimeListType);
        if (listpath.isPresent()) {
            defaultProductionPaths.put(new Pair<>(listType, knimeListType), listpath.get());
        } else {
            LOGGER.error("Default production path is not available: " + listType + " -> " + knimeListType);
        }
    }

    @Override
    public String convertExternalTypeToString(final ParquetType externalType) {
       return externalType.toExternalLogicalTypeString();
    }

    @Override
    public ParquetType convertStringToExternalType(final String string) {
        return ParquetType.fromExternalLogicalTypeString(string);
    }
}
