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
 *   Mar 11, 2026 (Jochen Reissinger, TNG Technology Consulting GmbH): created
 */
package org.knime.bigdata.fileformats.orc.writer3;

import static org.knime.bigdata.fileformats.orc.writer3.OrcTypeMappingUtils.getIdForConsumptionPath;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.orc.TypeDescription;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCTypeMappingService;
import org.knime.bigdata.fileformats.orc.writer3.OrcTypeMappingUtils.FilterType;
import org.knime.bigdata.fileformats.orc.writer3.OrcTypeMappingUtils.OrcTypeChoicesProvider;
import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;
import org.knime.node.datatype.mapping.SettingsModelDataTypeMapping;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.array.ArrayWidget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.ConfigMigration;
import org.knime.node.parameters.migration.Migration;
import org.knime.node.parameters.migration.NodeParametersMigration;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.DataTypeChoicesProvider;

/**
 * Node parameters for type mapping part in ORC Writer. Backwards compatible to the settings structure of
 * {@link SettingsModelDataTypeMapping}. KNIME to External type mappings by name and type.
 *
 * @author Jochen Reissinger, TNG Technology Consulting GmbH
 */
@Persistor(OrcTypeMappingParameters.OrcTypeMappingPersistor.class)
@Migration(OrcTypeMappingParameters.OrcTypeMappingMigration.class)
@SuppressWarnings("restriction")
final class OrcTypeMappingParameters implements NodeParameters {

    OrcTypeMappingParameters() {
        // default constructor
    }

    OrcTypeMappingParameters(final OrcByNameMappingSettings[] byNameSettings,
        final OrcByTypeMappingSettings[] byTypeSettings) {
        m_byNameSettings = byNameSettings;
        m_byTypeSettings = byTypeSettings;
    }

    @Section(title = "Mapping by Name", description = """
            Define name-based mappings that apply to specific columns. \
            """)
    private interface MappingByName {
    }

    @Section(title = "Mapping by Type", description = """
            Define type mappings that apply to all columns of the specified KNIME type. These mappings are applied \
            after the mappings defined in the 'Mapping by Name' section.
            """)
    @After(OrcTypeMappingParameters.MappingByName.class)
    private interface MappingByType {
    }

    @Widget(title = "Name", description = """
            Columns that match the given name (or regular expression) and KNIME type will be mapped to the \
            specified ORC type.
            """)
    @ArrayWidget(addButtonText = "Add name", elementTitle = "Column name")
    @Layout(OrcTypeMappingParameters.MappingByName.class)
    @Modification(OrcTypeMappingParameters.ByNameModification.class)
    OrcByNameMappingSettings[] m_byNameSettings = new OrcByNameMappingSettings[0];

    private static final class ByNameModification implements Modification.Modifier {

        @Override
        public void modify(final Modification.WidgetGroupModifier group) {
            group.find(OrcByNameMappingSettings.FromColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByNameKnimeTypeChoicesProvider.class).modify();

            group.find(OrcByNameMappingSettings.ToColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByNameOrcTypeChoicesProvider.class).modify();

            group.find(OrcByNameMappingSettings.ToColTypeRef.class).addAnnotation(ValueProvider.class)
                .withValue(ByNameOrcTypeValueProvider.class).modify();
        }

        private static final class ByNameKnimeTypeChoicesProvider implements DataTypeChoicesProvider {

            @Override
            public List<DataType> choices(final NodeParametersInput context) {
                final var mappingService = ORCTypeMappingService.getInstance();
                return mappingService.getKnimeSourceTypes().stream()
                    .sorted(Comparator.comparing(DataType::toPrettyString)).toList();
            }
        }

        private static final class ByNameOrcTypeChoicesProvider
            extends OrcTypeChoicesProvider<OrcByNameMappingSettings.FromColTypeRef> {

            ByNameOrcTypeChoicesProvider() {
                super(OrcByNameMappingSettings.FromColTypeRef.class);
            }
        }

        private static final class ByNameOrcTypeValueProvider implements StateProvider<String> {

            private Supplier<String> m_fromColumn;

            private Supplier<DataType> m_fromType;

            @Override
            public void init(final StateProviderInitializer initializer) {
                m_fromColumn = initializer.computeFromValueSupplier(OrcByNameMappingSettings.FromColRef.class);
                m_fromType = initializer.computeFromValueSupplier(OrcByNameMappingSettings.FromColTypeRef.class);
            }

            @Override
            public String computeState(final NodeParametersInput context)
                throws StateComputationFailureException {
                final String colName = m_fromColumn.get();
                final DataType dataType = m_fromType.get();
                if (colName == null || dataType == null) {
                    return "";
                }
                final var mappingService = ORCTypeMappingService.getInstance();
                final var consumptionPaths = mappingService.getConsumptionPathsFor(dataType);
                final var path = consumptionPaths.stream().findFirst();
                return path.map(OrcTypeMappingUtils::getIdForConsumptionPath).orElse("");
            }
        }
    }

    @Widget(title = "Type",
        description = "Columns that match the given KNIME type will be mapped to the specified ORC data type.")
    @ArrayWidget(addButtonText = "Add type", elementTitle = "Type")
    @ValueReference(OrcTypeMappingParameters.ByTypeRef.class)
    @Layout(OrcTypeMappingParameters.MappingByType.class)
    @Modification(OrcTypeMappingParameters.ByTypeModification.class)
    OrcByTypeMappingSettings[] m_byTypeSettings = new OrcByTypeMappingSettings[0];

    private static final class ByTypeModification implements Modification.Modifier {

        @Override
        public void modify(final Modification.WidgetGroupModifier group) {
            group.find(OrcByTypeMappingSettings.FromColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByTypeKnimeTypeChoicesProvider.class).modify();

            group.find(OrcByTypeMappingSettings.ToColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByTypeOrcTypeChoicesProvider.class).modify();

            group.find(OrcByTypeMappingSettings.ToColTypeRef.class).addAnnotation(ValueProvider.class)
                .withValue(ByTypeOrcTypeValueProvider.class).modify();
        }

        private static final class ByTypeKnimeTypeChoicesProvider implements DataTypeChoicesProvider {

            private Supplier<DataType> m_fromType;

            private Supplier<OrcByTypeMappingSettings[]> m_array;

            @Override
            public void init(final StateProviderInitializer initializer) {
                this.m_fromType =
                    initializer.computeFromValueSupplier(OrcByTypeMappingSettings.FromColTypeRef.class);
                this.m_array = initializer.computeFromValueSupplier(OrcTypeMappingParameters.ByTypeRef.class);
                initializer.computeBeforeOpenDialog();
            }

            @Override
            public List<DataType> choices(final NodeParametersInput context) {
                final var mappingService = ORCTypeMappingService.getInstance();
                final var existingTypes = Arrays.stream(m_array.get()).map(setting -> setting.m_fromType)
                    .filter(type -> type != null && !type.equals(this.m_fromType.get()))
                    .collect(Collectors.toSet());
                return mappingService.getKnimeSourceTypes().stream()
                    .filter(type -> !existingTypes.contains(type))
                    .sorted(Comparator.comparing(DataType::toPrettyString)).toList();
            }
        }

        private static final class ByTypeOrcTypeChoicesProvider
            extends OrcTypeChoicesProvider<OrcByTypeMappingSettings.FromColTypeRef> {

            ByTypeOrcTypeChoicesProvider() {
                super(OrcByTypeMappingSettings.FromColTypeRef.class);
            }
        }

        private static final class ByTypeOrcTypeValueProvider implements StateProvider<String> {

            private Supplier<DataType> m_fromType;

            @Override
            public void init(final StateProviderInitializer initializer) {
                m_fromType =
                    initializer.computeFromValueSupplier(OrcByTypeMappingSettings.FromColTypeRef.class);
            }

            @Override
            public String computeState(final NodeParametersInput context)
                throws StateComputationFailureException {
                final DataType dataType = m_fromType.get();
                if (dataType == null) {
                    return "";
                }
                final var mappingService = ORCTypeMappingService.getInstance();
                final var consumptionPaths = mappingService.getConsumptionPathsFor(dataType);
                final var path = consumptionPaths.stream().findFirst();
                return path.map(OrcTypeMappingUtils::getIdForConsumptionPath).orElse("");
            }
        }
    }

    private interface ByTypeRef extends ParameterReference<OrcByTypeMappingSettings[]> {
    }

    static final class OrcTypeMappingPersistor implements NodeParametersPersistor<OrcTypeMappingParameters> {

        @Override
        public OrcTypeMappingParameters load(final NodeSettingsRO settings) throws InvalidSettingsException {

            final var configData = DataTypeMappingConfigurationData.from(settings);
            final var config = configData.resolve(ORCTypeMappingService.getInstance(),
                DataTypeMappingDirection.KNIME_TO_EXTERNAL);

            final var byNameSettings = config.getNameRules().stream()//
                .filter(DataTypeMappingConfiguration.Rule::isValid)//
                .map(OrcTypeMappingPersistor::toByNameMappingSettings)//
                .toArray(OrcByNameMappingSettings[]::new);

            final var byTypeSettings = config.getTypeRules().stream()//
                .filter(DataTypeMappingConfiguration.Rule::isValid)//
                .map(OrcTypeMappingPersistor::toByTypeMappingSettings)//
                .toArray(OrcByTypeMappingSettings[]::new);

            return new OrcTypeMappingParameters(byNameSettings, byTypeSettings);
        }

        private static OrcByNameMappingSettings
            toByNameMappingSettings(final DataTypeMappingConfiguration<TypeDescription>.Rule rule) {
            final var filterType = rule.isRegex() ? FilterType.REGEX : FilterType.MANUAL;
            final var columnName = rule.getColumnName();
            final var knimeType = rule.getKnimeType();
            final var consumptionPath = rule.getConsumptionPath();
            return new OrcByNameMappingSettings(filterType, columnName, knimeType, consumptionPath);
        }

        private static OrcByTypeMappingSettings
            toByTypeMappingSettings(final DataTypeMappingConfiguration<TypeDescription>.Rule rule) {
            final var knimeType = rule.getKnimeType();
            final var consumptionPath = rule.getConsumptionPath();
            return new OrcByTypeMappingSettings(knimeType, consumptionPath);
        }

        @Override
        public void save(final OrcTypeMappingParameters params, final NodeSettingsWO settings) {
            final var mappingService = ORCTypeMappingService.getInstance();
            final var config =
                mappingService.createMappingConfiguration(DataTypeMappingDirection.KNIME_TO_EXTERNAL);
            Arrays.stream(params.m_byNameSettings).forEach(s -> {
                var matchingPath = findMatchingConsumptionPath(s.m_fromColType, s.m_toColType);
                matchingPath.ifPresent(consumptionPath -> config.addRule(s.m_fromColName,
                    s.m_filterType == FilterType.REGEX, s.m_fromColType, consumptionPath));
            });
            Arrays.stream(params.m_byTypeSettings).forEach(s -> {
                var matchingPath = findMatchingConsumptionPath(s.m_fromType, s.m_toType);
                matchingPath.ifPresent(consumptionPath -> config.addRule(s.m_fromType, consumptionPath));
            });

            DataTypeMappingConfigurationData.from(config).copyTo(settings);
        }

        private static Optional<ConsumptionPath> findMatchingConsumptionPath(final DataType fromType,
            final String toTypePathString) {
            final var mappingService = ORCTypeMappingService.getInstance();
            final var consumptionPaths = mappingService.getConsumptionPathsFor(fromType);
            return consumptionPaths.stream()
                .filter(path -> getIdForConsumptionPath(path).equals(toTypePathString)).findFirst();
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{};
        }
    }

    static class OrcTypeMappingMigration implements NodeParametersMigration<OrcTypeMappingParameters> {

        OrcTypeMappingMigration() {
            super();
        }

        @Override
        public List<ConfigMigration<OrcTypeMappingParameters>> getConfigMigrations() {
            return List
                .of(ConfigMigration.builder(settings -> (OrcTypeMappingParameters)null)
                    .withMatcher(settings -> false)
                    .withDeprecatedConfigPath("name_to_type_mapping_rules", "type_to_type_mapping_rules").build());
        }
    }

}
