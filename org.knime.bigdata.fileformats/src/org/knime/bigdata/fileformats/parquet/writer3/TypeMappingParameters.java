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
 *   Jan 8, 2026 (Jochen Reißinger, TNG Technology Consulting GmbH): created
 */
package org.knime.bigdata.fileformats.parquet.writer3;

import static org.knime.bigdata.fileformats.parquet.writer3.TypeMappingUtils.CFG_CONSUMER_PATH;
import static org.knime.bigdata.fileformats.parquet.writer3.TypeMappingUtils.CFG_CONVERTER_PATH;
import static org.knime.bigdata.fileformats.parquet.writer3.TypeMappingUtils.formatStringPair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeMappingService;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMappingSettings.ByNameMappingSettings;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMappingSettings.ByTypeMappingSettings;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMappingUtils.FilterType;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMappingUtils.ParquetTypeChoicesProvider;
import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.array.ArrayWidget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.DataTypeChoicesProvider;

/**
 * @author Jochen Reißinger, TNG Technology Consulting GmbH
 */
@SuppressWarnings("javadoc")
public final class TypeMappingParameters implements NodeParameters {

    @Section(title = "Mapping by Name")
    private interface MappingByName {
    }

    @Section(title = "Mapping by Type")
    @After(TypeMappingParameters.MappingByName.class)
    private interface MappingByType {
    }

    @Widget(title = "Name", description = """
            Columns that match the given name (or regular expression) and KNIME type will be mapped to the \
            specified database type.
            """)
    @ArrayWidget(addButtonText = "Add name", elementTitle = "Column name")
    @Layout(TypeMappingParameters.MappingByName.class)
    @Modification(TypeMappingParameters.ByNameModification.class)
    @Persistor(ByNameMappingPersistor.class)
    ByNameMappingSettings[] m_byNameSettings = new ByNameMappingSettings[0];

    private static final class ByNameModification implements Modification.Modifier {

        @Override
        public void modify(final Modification.WidgetGroupModifier group) {
            group.find(ByNameMappingSettings.FromColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByNameKnimeTypeChoicesProvider.class).modify();

            group.find(ByNameMappingSettings.ToColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByNameParquetTypeChoicesProvider.class).modify();

            group.find(ByNameMappingSettings.ToColTypeRef.class).addAnnotation(ValueProvider.class)
                .withValue(ByNameParquetTypeValueProvider.class).modify();
        }

    }

    private static final class ByNameKnimeTypeChoicesProvider implements DataTypeChoicesProvider {

        @Override
        public List<DataType> choices(final NodeParametersInput context) {
            final var mappingService = ParquetLogicalTypeMappingService.getInstance();
            return mappingService.getKnimeSourceTypes().stream()
                .sorted((t1, t2) -> t1.toPrettyString().compareTo(t2.toPrettyString())).toList();
        }
    }

    private static final class ByNameParquetTypeChoicesProvider
        extends ParquetTypeChoicesProvider<ByNameMappingSettings.FromColTypeRef> {

        ByNameParquetTypeChoicesProvider() {
            super(ByNameMappingSettings.FromColTypeRef.class);
        }
    }

    private static final class ByNameParquetTypeValueProvider implements StateProvider<String> {

        private Supplier<String> m_fromColumn;

        private Supplier<DataType> m_fromType;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_fromColumn = initializer.computeFromValueSupplier(ByNameMappingSettings.FromColRef.class);
            m_fromType = initializer.computeFromValueSupplier(ByNameMappingSettings.FromColTypeRef.class);
        }

        @Override
        public String computeState(final NodeParametersInput context) throws StateComputationFailureException {
            final String colName = m_fromColumn.get();
            final DataType dataType = m_fromType.get();
            if (colName == null || dataType == null) {
                return "";
            }
            final var mappingService = ParquetLogicalTypeMappingService.getInstance();
            final var consumptionPaths = mappingService.getConsumptionPathsFor(dataType);
            final var path = consumptionPaths.stream().findFirst();
            if (path.isEmpty()) {
                return "";
            }
            return formatStringPair(path.get().getConverterFactory().getIdentifier(),
                path.get().getConsumerFactory().getIdentifier());
        }

    }

    @Widget(title = "Type",
        description = "Columns that match the given KNIME type will be mapped to the specified database type.")
    @ArrayWidget(addButtonText = "Add type", elementTitle = "Type")
    @ValueReference(TypeMappingParameters.ByTypeRef.class)
    @Layout(TypeMappingParameters.MappingByType.class)
    @Modification(TypeMappingParameters.ByTypeModification.class)
    @Persistor(ByTypeMappingPersistor.class)
    ByTypeMappingSettings[] m_byTypeSettings = new ByTypeMappingSettings[0];

    private static final class ByTypeModification implements Modification.Modifier {

        @Override
        public void modify(final Modification.WidgetGroupModifier group) {
            group.find(ByTypeMappingSettings.FromColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByTypeKnimeTypeChoicesProvider.class).modify();

            group.find(ByTypeMappingSettings.ToColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByTypeParquetTypeChoicesProvider.class).modify();

            group.find(ByTypeMappingSettings.ToColTypeRef.class).addAnnotation(ValueProvider.class)
                .withValue(ByTypeParquetTypeValueProvider.class).modify();
        }

    }

    private interface ByTypeRef extends ParameterReference<ByTypeMappingSettings[]> {
    }

    private static final class ByTypeKnimeTypeChoicesProvider implements DataTypeChoicesProvider {

        private Supplier<DataType> m_fromType;

        private Supplier<ByTypeMappingSettings[]> m_array;

        @Override
        public void init(final StateProviderInitializer initializer) {
            this.m_fromType = initializer.computeFromValueSupplier(ByTypeMappingSettings.FromColTypeRef.class);
            this.m_array = initializer.computeFromValueSupplier(TypeMappingParameters.ByTypeRef.class);
            initializer.computeBeforeOpenDialog();
        }

        @Override
        public List<DataType> choices(final NodeParametersInput context) {
            final var mappingService = ParquetLogicalTypeMappingService.getInstance();
            final var existingTypes = Arrays.stream(m_array.get()).map(setting -> setting.m_fromType)
                .filter(type -> type != null && !type.equals(this.m_fromType.get())).collect(Collectors.toSet());
            return mappingService.getKnimeSourceTypes().stream().filter(type -> !existingTypes.contains(type))
                .sorted((t1, t2) -> t1.toPrettyString().compareTo(t2.toPrettyString())).toList();
        }

    }

    private static final class ByTypeParquetTypeChoicesProvider
        extends ParquetTypeChoicesProvider<ByTypeMappingSettings.FromColTypeRef> {

        ByTypeParquetTypeChoicesProvider() {
            super(ByTypeMappingSettings.FromColTypeRef.class);
        }
    }

    private static final class ByTypeParquetTypeValueProvider implements StateProvider<String> {

        private Supplier<DataType> m_fromType;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_fromType = initializer.computeFromValueSupplier(ByTypeMappingSettings.FromColTypeRef.class);
        }

        @Override
        public String computeState(final NodeParametersInput context) throws StateComputationFailureException {
            final DataType dataType = m_fromType.get();
            if (dataType == null) {
                return "";
            }
            final var mappingService = ParquetLogicalTypeMappingService.getInstance();
            final var consumptionPaths = mappingService.getConsumptionPathsFor(dataType);
            final var path = consumptionPaths.stream().findFirst();
            if (path.isEmpty()) {
                return "";
            }
            return formatStringPair(path.get().getConverterFactory().getIdentifier(),
                path.get().getConsumerFactory().getIdentifier());
        }

    }

    // Shared constants and methods for persistors
    private static final String CFG_NAME = "name";

    private static final String CFG_REGEX = "regex";

    private static final String CFG_TYPE = "type";

    private static final String CFG_CELL_CLASS = "cell_class";

    private static DataType loadKnimeType(final NodeSettingsRO ruleSettings) throws InvalidSettingsException {
        final var typeSettings = ruleSettings.getNodeSettings(CFG_TYPE);
        final var cellClassName = typeSettings.getString(CFG_CELL_CLASS);

        try {
            @SuppressWarnings("unchecked")
            final Class<? extends org.knime.core.data.DataCell> cellClass =
                (Class<? extends org.knime.core.data.DataCell>)Class.forName(cellClassName);

            if (typeSettings.containsKey("collection_element_type")) {
                final var elementTypeSettings = typeSettings.getNodeSettings("collection_element_type");
                final var elementCellClassName = elementTypeSettings.getString(CFG_CELL_CLASS);
                @SuppressWarnings("unchecked")
                final Class<? extends org.knime.core.data.DataCell> elementCellClass =
                    (Class<? extends org.knime.core.data.DataCell>)Class.forName(elementCellClassName);
                final var elementType = DataType.getType(elementCellClass);
                return DataType.getType(cellClass, elementType);
            } else {
                return DataType.getType(cellClass);
            }
        } catch (ClassNotFoundException e) {
            System.err.println("Skipping type mapping rule: Cell class not found: " + cellClassName);
            return null;
        }
    }

    private static void saveRuleCommonFields(final NodeSettingsWO ruleSettings, final DataType knimeType,
        final ConsumptionPath path) {

        final var typeSettings = ruleSettings.addNodeSettings(CFG_TYPE);
        if (knimeType.isCollectionType() && knimeType.getCollectionElementType() != null) {
            final var elementType = knimeType.getCollectionElementType();
            final var elementTypeSettings = typeSettings.addNodeSettings("collection_element_type");
            elementTypeSettings.addString(CFG_CELL_CLASS, elementType.getCellClass().getName());
        }
        typeSettings.addString(CFG_CELL_CLASS, knimeType.getCellClass().getName());

        ruleSettings.addString("intermediate_type", path.getConverterFactory().getDestinationType().getName());

        final var consumer = path.getConsumerFactory();
        ruleSettings.addString("external_type", consumer.getDestinationType().toString());

        final var converter = path.getConverterFactory();
        ruleSettings.addString(CFG_CONVERTER_PATH, converter.getIdentifier());
        ruleSettings.addString(CFG_CONVERTER_PATH + "_src", converter.getSourceType().getName());
        ruleSettings.addString(CFG_CONVERTER_PATH + "_dst", converter.getDestinationType().getName());
        ruleSettings.addString(CFG_CONVERTER_PATH + "_name", converter.getName());
        ruleSettings.addNodeSettings(CFG_CONVERTER_PATH + "_config");

        ruleSettings.addString(CFG_CONSUMER_PATH, consumer.getIdentifier());
        ruleSettings.addString(CFG_CONSUMER_PATH + "_src", consumer.getSourceType().getName());
        ruleSettings.addString(CFG_CONSUMER_PATH + "_dst", consumer.getDestinationType().toString());
        ruleSettings.addString(CFG_CONSUMER_PATH + "_name", consumer.getName());
        ruleSettings.addNodeSettings(CFG_CONSUMER_PATH + "_config");
    }

    private static final class ByNameMappingPersistor implements NodeParametersPersistor<ByNameMappingSettings[]> {

        private static final String CFG_NAME_TO_TYPE_RULES = "name_to_type_mapping_rules";

        private static final String CFG_NAME_TO_TYPE_RULE = "name_to_type_mapping_rule_";

        @Override
        public ByNameMappingSettings[] load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final var byNameList = new ArrayList<ByNameMappingSettings>();

            if (settings.containsKey(CFG_NAME_TO_TYPE_RULES)) {
                final var rulesSettings = settings.getNodeSettings(CFG_NAME_TO_TYPE_RULES);
                int index = 0;
                while (rulesSettings.containsKey(CFG_NAME_TO_TYPE_RULE + index)) {
                    final var ruleSettings = rulesSettings.getNodeSettings(CFG_NAME_TO_TYPE_RULE + index);
                    loadNameBasedRule(ruleSettings, byNameList);
                    index++;
                }
            }

            return byNameList.toArray(new ByNameMappingSettings[0]);
        }

        private static void loadNameBasedRule(final NodeSettingsRO ruleSettings,
            final ArrayList<ByNameMappingSettings> byNameList) throws InvalidSettingsException {
            final var knimeType = TypeMappingParameters.loadKnimeType(ruleSettings);
            if (knimeType == null) {
                return;
            }

            final var converterPath = ruleSettings.getString(CFG_CONVERTER_PATH);
            final var consumerPath = ruleSettings.getString(CFG_CONSUMER_PATH);
            final var fullPath = String.format("%s;%s", converterPath, consumerPath);

            final var columnName = ruleSettings.getString(CFG_NAME);
            final var isRegex = ruleSettings.getBoolean(CFG_REGEX, false);

            final var filterType = isRegex ? FilterType.REGEX : FilterType.MANUAL;
            final var byNameSetting = new ByNameMappingSettings(filterType, columnName, knimeType, fullPath);
            byNameList.add(byNameSetting);
        }

        @Override
        public void save(final ByNameMappingSettings[] obj, final NodeSettingsWO settings) {
            final var mappingService = ParquetLogicalTypeMappingService.getInstance();

            final var nameRulesSettings = settings.addNodeSettings(CFG_NAME_TO_TYPE_RULES);
            int nameRuleIndex = 0;
            for (final var byNameSetting : obj) {
                if (byNameSetting.m_fromColType == null || byNameSetting.m_toColType == null) {
                    continue;
                }

                final var consumptionPaths = mappingService.getConsumptionPathsFor(byNameSetting.m_fromColType);
                final var matchingPath = consumptionPaths.stream().filter(path -> {
                    final var pathString = formatStringPair(path.getConverterFactory().getIdentifier(),
                        path.getConsumerFactory().getIdentifier());
                    return pathString.equals(byNameSetting.m_toColType);
                }).findFirst();

                if (matchingPath.isEmpty()) {
                    continue;
                }

                final var path = matchingPath.get();
                final var ruleSettings = nameRulesSettings.addNodeSettings(CFG_NAME_TO_TYPE_RULE + nameRuleIndex);

                ruleSettings.addString(CFG_NAME, byNameSetting.m_fromColName);
                ruleSettings.addBoolean(CFG_REGEX, byNameSetting.m_filterType == FilterType.REGEX);

                saveRuleCommonFields(ruleSettings, byNameSetting.m_fromColType, path);

                nameRuleIndex++;
            }
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{new String[]{CFG_NAME_TO_TYPE_RULES}};
        }
    }

    private static final class ByTypeMappingPersistor implements NodeParametersPersistor<ByTypeMappingSettings[]> {

        private static final String CFG_TYPE_TO_TYPE_RULES = "type_to_type_mapping_rules";

        private static final String CFG_TYPE_TO_TYPE_RULE = "type_to_type_mapping_rule_";

        @Override
        public ByTypeMappingSettings[] load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final var byTypeList = new ArrayList<ByTypeMappingSettings>();

            if (settings.containsKey(CFG_TYPE_TO_TYPE_RULES)) {
                final var rulesSettings = settings.getNodeSettings(CFG_TYPE_TO_TYPE_RULES);
                int index = 0;
                while (rulesSettings.containsKey(CFG_TYPE_TO_TYPE_RULE + index)) {
                    final var ruleSettings = rulesSettings.getNodeSettings(CFG_TYPE_TO_TYPE_RULE + index);
                    loadTypeBasedRule(ruleSettings, byTypeList);
                    index++;
                }
            }

            return byTypeList.toArray(new ByTypeMappingSettings[0]);
        }

        private static void loadTypeBasedRule(final NodeSettingsRO ruleSettings,
            final ArrayList<ByTypeMappingSettings> byTypeList) throws InvalidSettingsException {
            final var knimeType = TypeMappingParameters.loadKnimeType(ruleSettings);
            if (knimeType == null) {
                return;
            }

            final var converterPath = ruleSettings.getString(CFG_CONVERTER_PATH);
            final var consumerPath = ruleSettings.getString(CFG_CONSUMER_PATH);
            final var fullPath = formatStringPair(converterPath, consumerPath);

            final var byTypeSetting = new ByTypeMappingSettings(knimeType, fullPath);
            byTypeList.add(byTypeSetting);
        }

        @Override
        public void save(final ByTypeMappingSettings[] obj, final NodeSettingsWO settings) {
            final var mappingService = ParquetLogicalTypeMappingService.getInstance();

            final var typeRulesSettings = settings.addNodeSettings(CFG_TYPE_TO_TYPE_RULES);
            int typeRuleIndex = 0;
            for (final var byTypeSetting : obj) {
                if (byTypeSetting.m_fromType == null || byTypeSetting.m_toType == null) {
                    continue;
                }

                final var consumptionPaths = mappingService.getConsumptionPathsFor(byTypeSetting.m_fromType);
                final var matchingPath = consumptionPaths.stream().filter(path -> {
                    final var pathString = formatStringPair(path.getConverterFactory().getIdentifier(),
                        path.getConsumerFactory().getIdentifier());
                    return pathString.equals(byTypeSetting.m_toType);
                }).findFirst();

                if (matchingPath.isEmpty()) {
                    continue;
                }

                final var path = matchingPath.get();
                final var ruleSettings = typeRulesSettings.addNodeSettings(CFG_TYPE_TO_TYPE_RULE + typeRuleIndex);

                saveRuleCommonFields(ruleSettings, byTypeSetting.m_fromType, path);

                typeRuleIndex++;
            }
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{new String[]{CFG_TYPE_TO_TYPE_RULES}};
        }
    }

}
