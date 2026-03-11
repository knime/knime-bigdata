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
 *   Mar 11, 2026 (Jochen Reißinger, TNG Technology Consulting GmbH): created
 */
package org.knime.bigdata.fileformats.orc.writer2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.orc.TypeDescription;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCTypeMappingService;
import org.knime.bigdata.fileformats.utility.ByNameMappingSettings;
import org.knime.bigdata.fileformats.utility.ByTypeMappingSettings;
import org.knime.bigdata.fileformats.utility.TypeMappingParameters;
import org.knime.bigdata.fileformats.utility.TypeMappingUtils;
import org.knime.bigdata.fileformats.utility.TypeMappingUtils.TypeChoicesProvider;
import org.knime.core.data.DataType;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.node.datatype.mapping.SettingsModelDataTypeMapping;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.migration.ConfigMigration;
import org.knime.node.parameters.migration.Migration;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.DataTypeChoicesProvider;

/**
 * Node parameters for type mapping part in ORC Writer. Backwards compatible to the settings structure of
 * {@link SettingsModelDataTypeMapping}. KNIME to External type mappings by name and type.
 *
 * @author Jochen Reißinger, TNG Technology Consulting GmbH
 */
@Persistor(OrcTypeMappingParameters.OrcTypeMappingPersistor.class)
@Migration(OrcTypeMappingParameters.OrcTypeMappingMigration.class)
@Modification({OrcTypeMappingParameters.ByNameModification.class, OrcTypeMappingParameters.ByTypeModification.class})
@SuppressWarnings("restriction")
final class OrcTypeMappingParameters extends TypeMappingParameters {

    private static final DataTypeMappingService<TypeDescription, ?, ?> MAPPING_SERVICE =
        ORCTypeMappingService.getInstance();

    OrcTypeMappingParameters() {
        // default constructor
    }

    OrcTypeMappingParameters(final ByNameMappingSettings[] byNameSettings,
        final ByTypeMappingSettings[] byTypeSettings) {
        super(byNameSettings, byTypeSettings);
    }

    static final class ByNameModification implements Modification.Modifier {

        @Override
        public void modify(final Modification.WidgetGroupModifier group) {
            group.find(ByNameMappingSettings.FromColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByNameKnimeTypeChoicesProvider.class).modify();

            group.find(ByNameMappingSettings.ToColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByNameOrcTypeChoicesProvider.class).modify();

            group.find(ByNameMappingSettings.ToColTypeRef.class).addAnnotation(ValueProvider.class)
                .withValue(ByNameOrcTypeValueProvider.class).modify();
        }

        private static final class ByNameKnimeTypeChoicesProvider implements DataTypeChoicesProvider {

            @Override
            public List<DataType> choices(final NodeParametersInput context) {
                return MAPPING_SERVICE.getKnimeSourceTypes().stream()
                    .sorted(Comparator.comparing(DataType::toPrettyString)).toList();
            }
        }

        private static final class ByNameOrcTypeChoicesProvider
            extends TypeChoicesProvider<TypeDescription, ByNameMappingSettings.FromColTypeRef> {

            ByNameOrcTypeChoicesProvider() {
                super(ByNameMappingSettings.FromColTypeRef.class);
            }

            @Override
            protected DataTypeMappingService<TypeDescription, ?, ?> getMappingService() {
                return MAPPING_SERVICE;
            }
        }

        private static final class ByNameOrcTypeValueProvider implements StateProvider<String> {

            private Supplier<DataType> m_fromType;

            @Override
            public void init(final StateProviderInitializer initializer) {
                m_fromType = initializer.computeFromValueSupplier(ByNameMappingSettings.FromColTypeRef.class);
            }

            @Override
            public String computeState(final NodeParametersInput context) throws StateComputationFailureException {
                final DataType dataType = m_fromType.get();
                if (dataType == null) {
                    return "";
                }
                final var consumptionPaths = MAPPING_SERVICE.getConsumptionPathsFor(dataType);
                final var path = consumptionPaths.stream().findFirst();
                return path.map(TypeMappingUtils::getIdForConsumptionPath).orElse("");
            }

        }

    }

    static final class ByTypeModification implements Modification.Modifier {

        @Override
        public void modify(final Modification.WidgetGroupModifier group) {
            group.find(ByTypeRef.class).addAnnotation(ValueProvider.class)
                .withValue(DefaultByTypeMappingsProvider.class).modify();

            group.find(ByTypeMappingSettings.FromColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByTypeKnimeTypeChoicesProvider.class).modify();

            group.find(ByTypeMappingSettings.ToColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByTypeOrcTypeChoicesProvider.class).modify();

            group.find(ByTypeMappingSettings.ToColTypeRef.class).addAnnotation(ValueProvider.class)
                .withValue(ByTypeOrcTypeValueProvider.class).modify();
        }

        private static final class DefaultByTypeMappingsProvider implements StateProvider<ByTypeMappingSettings[]> {

            private Supplier<ByTypeMappingSettings[]> m_current;

            @Override
            public void init(final StateProviderInitializer initializer) {
                m_current = initializer.getValueSupplier(ByTypeRef.class);
                initializer.computeBeforeOpenDialog();
            }

            @Override
            public ByTypeMappingSettings[] computeState(final NodeParametersInput context)
                throws StateComputationFailureException {
                if (m_current.get().length > 0) {
                    // already has settings (loaded from saved node) — don't overwrite
                    throw new StateComputationFailureException();
                }
                var mappings = MAPPING_SERVICE.newDefaultKnimeToExternalMappingConfiguration().getTypeRules().stream()
                    .filter(DataTypeMappingConfiguration.Rule::isValid)
                    .map(rule -> new ByTypeMappingSettings(rule.getKnimeType(), rule.getConsumptionPath()))
                    .toArray(ByTypeMappingSettings[]::new);
                return mappings;
            }
        }

        private static final class ByTypeKnimeTypeChoicesProvider implements DataTypeChoicesProvider {

            private Supplier<DataType> m_fromType;

            private Supplier<ByTypeMappingSettings[]> m_array;

            @Override
            public void init(final StateProviderInitializer initializer) {
                this.m_fromType = initializer.computeFromValueSupplier(ByTypeMappingSettings.FromColTypeRef.class);
                this.m_array = initializer.computeFromValueSupplier(ByTypeRef.class);
                initializer.computeBeforeOpenDialog();
            }

            @Override
            public List<DataType> choices(final NodeParametersInput context) {
                final var existingTypes = Arrays.stream(m_array.get()).map(setting -> setting.m_fromType)
                    .filter(type -> type != null && !type.equals(this.m_fromType.get())).collect(Collectors.toSet());
                return MAPPING_SERVICE.getKnimeSourceTypes().stream().filter(type -> !existingTypes.contains(type))
                    .sorted(Comparator.comparing(DataType::toPrettyString)).toList();
            }

        }

        private static final class ByTypeOrcTypeChoicesProvider
            extends TypeChoicesProvider<TypeDescription, ByTypeMappingSettings.FromColTypeRef> {

            ByTypeOrcTypeChoicesProvider() {
                super(ByTypeMappingSettings.FromColTypeRef.class);
            }

            @Override
            protected DataTypeMappingService<TypeDescription, ?, ?> getMappingService() {
                return MAPPING_SERVICE;
            }
        }

        private static final class ByTypeOrcTypeValueProvider implements StateProvider<String> {

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
                final var consumptionPaths = MAPPING_SERVICE.getConsumptionPathsFor(dataType);
                final var path = consumptionPaths.stream().findFirst();
                return path.map(TypeMappingUtils::getIdForConsumptionPath).orElse("");
            }

        }
    }

    static final class OrcTypeMappingPersistor extends TypeMappingPersistor<OrcTypeMappingParameters> {

        @Override
        protected DataTypeMappingService<?, ?, ?> getMappingService() {
            return MAPPING_SERVICE;
        }

        @Override
        protected OrcTypeMappingParameters create(final ByNameMappingSettings[] byNameSettings,
            final ByTypeMappingSettings[] byTypeSettings) {
            return new OrcTypeMappingParameters(byNameSettings, byTypeSettings);
        }
    }

    static final class OrcTypeMappingMigration extends TypeMappingMigration<OrcTypeMappingParameters> {

        @Override
        public List<ConfigMigration<OrcTypeMappingParameters>> getConfigMigrations() {
            return List
                .of(ConfigMigration.builder(settings -> (OrcTypeMappingParameters)null).withMatcher(settings -> false)
                    .withDeprecatedConfigPath("name_to_type_mapping_rules", "type_to_type_mapping_rules").build());
        }
    }

}
