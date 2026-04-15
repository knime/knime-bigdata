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
package org.knime.bigdata.fileformats.utility;

import static org.knime.bigdata.fileformats.utility.TypeMappingUtils.getIdForConsumptionPath;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.knime.bigdata.fileformats.utility.TypeMappingUtils.FilterType;
import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.data.convert.map.Destination;
import org.knime.core.data.convert.map.Source;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.node.parameters.modification.Modification;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;
import org.knime.node.datatype.mapping.SettingsModelDataTypeMapping;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.array.ArrayWidget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.ConfigMigration;
import org.knime.node.parameters.migration.NodeParametersMigration;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.ValueReference;

/**
 * Abstract base class for type mapping node parameters used in file format writer nodes. Backwards compatible to the
 * settings structure of {@link SettingsModelDataTypeMapping}. Provides KNIME to external type mappings by name and
 * type.
 * <p>
 * Concrete subclasses must:
 * <ul>
 * <li>declare a {@code private static final DataTypeMappingService<X, S, D> MAPPING_SERVICE} field</li>
 * <li>define concrete static inner modifier classes whose provider inner classes extend the reusable abstract bases in
 * {@link TypeMappingUtils} (e.g. {@link TypeMappingUtils.KnimeSourceTypeChoicesProvider},
 * {@link TypeMappingUtils.AvailableKnimeTypeChoicesProvider},
 * {@link TypeMappingUtils.AbstractExternalTypeValueProvider}, {@link TypeMappingUtils.TypeChoicesProvider})</li>
 * <li>annotate the subclass with {@code @Persistor} and {@code @Migration} referencing a concrete inner class
 * instantiating {@link TypeMappingPersistor} directly and a concrete inner class extending
 * {@link TypeMappingMigration}</li>
 * </ul>
 *
 * @author Jochen Reißinger, TNG Technology Consulting GmbH
 */
@SuppressWarnings({"restriction", "javadoc"})
public abstract class TypeMappingParameters implements NodeParameters {

    protected TypeMappingParameters() {
        // default constructor for concrete subclasses
    }

    protected TypeMappingParameters(final ByNameMappingSettings[] byNameSettings,
        final ByTypeMappingSettings[] byTypeSettings) {
        m_byNameSettings = byNameSettings;
        m_byTypeSettings = byTypeSettings;
    }

    @Section(title = "Mapping by Name", description = "Define name-based mappings that apply to specific columns.")
    public interface MappingByName {
    }

    @Section(title = "Mapping by Type", description = """
            Define type mappings that apply to all columns of the specified KNIME type. These mappings are applied \
            after the mappings defined in the 'Mapping by Name' section.
            """)
    @After(TypeMappingParameters.MappingByName.class)
    public interface MappingByType {
    }

    public interface ByTypeRef extends ParameterReference<ByTypeMappingSettings[]>, Modification.Reference {
    }

    @Widget(title = "Name", description = """
            Columns that match the given name (or regular expression) and KNIME type will be mapped to the \
            specified external type.
            """)
    @ArrayWidget(addButtonText = "Add name", elementTitle = "Column name")
    @Layout(TypeMappingParameters.MappingByName.class)
    public ByNameMappingSettings[] m_byNameSettings = new ByNameMappingSettings[0];

    @Widget(title = "Type",
        description = "Columns that match the given KNIME type will be mapped to the specified external data type.")
    @ArrayWidget(addButtonText = "Add type", elementTitle = "Type")
    @ValueReference(TypeMappingParameters.ByTypeRef.class)
    @Layout(TypeMappingParameters.MappingByType.class)
    public ByTypeMappingSettings[] m_byTypeSettings = new ByTypeMappingSettings[0];

    /**
     * Persistor for {@link TypeMappingParameters} subclasses. Subclasses pass the mapping service and an instance
     * factory to the constructor; no further overrides are needed.
     *
     * @param <P> the concrete {@link TypeMappingParameters} subclass
     * @param <X> the external type whose instances describe the target data types
     * @param <S> the external source type, must implement {@link Source}{@code <X>}
     * @param <D> the external destination type, must implement {@link Destination}{@code <X>}
     */
    public static class TypeMappingPersistor<P extends TypeMappingParameters, X, S extends Source<X>, D extends Destination<X>>
        implements NodeParametersPersistor<P> {

        private final DataTypeMappingService<X, S, D> m_mappingService;

        private final BiFunction<ByNameMappingSettings[], ByTypeMappingSettings[], P> m_factory;

        /**
         * @param mappingService the format-specific mapping service
         * @param factory a function that creates a new instance of the concrete parameters subclass from loaded by-name
         *            and by-type settings
         */
        public TypeMappingPersistor(final DataTypeMappingService<X, S, D> mappingService,
            final BiFunction<ByNameMappingSettings[], ByTypeMappingSettings[], P> factory) {
            this.m_mappingService = mappingService;
            this.m_factory = factory;
        }

        @Override
        public P load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final var configData = DataTypeMappingConfigurationData.from(settings);
            final var config = configData.resolve(m_mappingService, DataTypeMappingDirection.KNIME_TO_EXTERNAL);

            final var byNameSettings = config.getNameRules().stream().filter(DataTypeMappingConfiguration.Rule::isValid)
                .map(TypeMappingPersistor::toByNameMappingSettings).toArray(ByNameMappingSettings[]::new);

            final var byTypeSettings = config.getTypeRules().stream().filter(DataTypeMappingConfiguration.Rule::isValid)
                .map(TypeMappingPersistor::toByTypeMappingSettings).toArray(ByTypeMappingSettings[]::new);

            return m_factory.apply(byNameSettings, byTypeSettings);
        }

        private static ByNameMappingSettings toByNameMappingSettings(final DataTypeMappingConfiguration<?>.Rule rule) {
            final var filterType = rule.isRegex() ? FilterType.REGEX : FilterType.MANUAL;
            return new ByNameMappingSettings(filterType, rule.getColumnName(), rule.getKnimeType(),
                rule.getConsumptionPath());
        }

        private static ByTypeMappingSettings toByTypeMappingSettings(final DataTypeMappingConfiguration<?>.Rule rule) {
            return new ByTypeMappingSettings(rule.getKnimeType(), rule.getConsumptionPath());
        }

        @Override
        public void save(final P params, final NodeSettingsWO settings) {
            final var config = m_mappingService.createMappingConfiguration(DataTypeMappingDirection.KNIME_TO_EXTERNAL);
            Arrays.stream(params.m_byNameSettings)
                .forEach(s -> findMatchingConsumptionPath(s.m_fromColType, s.m_toColType).ifPresent(path -> config
                    .addRule(s.m_fromColName, s.m_filterType == FilterType.REGEX, s.m_fromColType, path)));
            Arrays.stream(params.m_byTypeSettings).forEach(s -> findMatchingConsumptionPath(s.m_fromType, s.m_toType)
                .ifPresent(path -> config.addRule(s.m_fromType, path)));
            DataTypeMappingConfigurationData.from(config).copyTo(settings);
        }

        private Optional<ConsumptionPath> findMatchingConsumptionPath(final DataType fromType,
            final String toTypePathString) {
            return m_mappingService.getConsumptionPathsFor(fromType).stream()
                .filter(path -> getIdForConsumptionPath(path).equals(toTypePathString)).findFirst();
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{};
        }
    }

    /**
     * Abstract base migration for {@link TypeMappingParameters} subclasses.
     *
     * @param <P> the concrete {@link TypeMappingParameters} subclass
     */
    public abstract static class TypeMappingMigration<P extends TypeMappingParameters>
        implements NodeParametersMigration<P> {

        @Override
        public List<ConfigMigration<P>> getConfigMigrations() {
            return List.of(ConfigMigration.builder(settings -> (P)null).withMatcher(settings -> false)
                .withDeprecatedConfigPath("name_to_type_mapping_rules", "type_to_type_mapping_rules").build());
        }
    }
}
