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
 *   Sep 3, 2025 (Paul Baernreuther): created
 */
package org.knime.bigdata.fileformats.parquet.writer3;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeMappingService;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMapping.ByNameMappingSettings;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMapping.ByNameMappingSettings.ByNameMappingModification;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMapping.ByTypeMappingSettings;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMapping.ByTypeMappingSettings.ByTypeMappingModification;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMapping.ByTypeMappingSettings.DynamicKnimeTypeChoicesProvider;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMappingUtils.ToDBTypeChoicesProvider;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.array.ArrayWidget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.DataTypeChoicesProvider;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;

/**
 * Settings for column type mapping used to overwrite default mappings.
 * <p>
 * These are currently used by all manipulator nodes and with that hard-coded to use the second input port.
 *
 * @author Jochen Reißinger, TNG Technology Consulting GmbH
 */
@SuppressWarnings("javadoc")
public final class TypeMappingParameters implements NodeParameters {

    @Section(title = "Mapping by Name")
    interface MappingByName {
    }

    @Section(title = "Mapping by Type")
    @After(TypeMappingParameters.MappingByName.class)
    interface MappingByType {
    }

    /**
     * Mapping settings by name.
     */
    @Widget(title = "Name", description = """
            Columns that match the given name (or regular expression) and KNIME type will be mapped to the \
            specified database type.
            """)
    @ArrayWidget(addButtonText = "Add name", elementTitle = "Column name")
    @Layout(TypeMappingParameters.MappingByName.class)
    @Modification(TypeMappingParameters.ByNameModification.class)
    public ByNameMappingSettings[] m_byNameSettings = new ByNameMappingSettings[0];

    static final class ByNameModification extends ByNameMappingModification {

        @Override
        protected Optional<Class<? extends DataTypeChoicesProvider>> getFromColTypeChoicesProvider() {
            return Optional.of(TypeMappingParameters.ByNameKnimeTypeChoicesProvider.class);
        }

        @Override
        protected Optional<Class<? extends StringChoicesProvider>> getToColTypeChoicesProvider() {
            return Optional.of(TypeMappingParameters.ToDBTypeByNameChoicesProvider.class);
        }

        @Override
        protected Optional<Class<? extends StateProvider<String>>> getToColTypeValueProvider() {
            return Optional.of(TypeMappingParameters.ToNameProvider.class);
        }

    }

    private static final class ByNameKnimeTypeChoicesProvider implements DataTypeChoicesProvider {

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeBeforeOpenDialog();
        }

        @Override
        public List<DataType> choices(final NodeParametersInput context) {
            // Get types from input table spec
            final var inSpec = context.getInPortSpec(0);
            if (inSpec.isPresent() && inSpec.get() instanceof DataTableSpec spec) {
                // Collect unique data types from all columns in the input table
                final var types = spec.stream()
                        .map(DataColumnSpec::getType)
                        .distinct()
                        .sorted((t1, t2) -> t1.toPrettyString().compareTo(t2.toPrettyString()))
                        .toList();
                if (!types.isEmpty()) {
                    return types;
                }
            }

            // Fallback: if no input spec or empty spec, show all supported types
            final var mappingService = ParquetLogicalTypeMappingService.getInstance();
            return mappingService.getKnimeSourceTypes().stream()
                    .sorted((t1, t2) -> t1.toPrettyString().compareTo(t2.toPrettyString()))
                    .toList();
        }
    }

    private static final class ToDBTypeByNameChoicesProvider extends
            ToDBTypeChoicesProvider<ByNameMappingSettings.FromColTypeRef> {

        ToDBTypeByNameChoicesProvider() {
            super(ByNameMappingSettings.FromColTypeRef.class);
        }
    }

    private static final class ToNameProvider implements StateProvider<String> {

        private Supplier<String> m_fromColumn;

        private Supplier<DataType> m_fromType;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_fromColumn = initializer.computeFromValueSupplier(ByNameMappingSettings.FromColRef.class);
            m_fromType = initializer.computeFromValueSupplier(ByNameMappingSettings.FromColTypeRef.class);
            initializer.computeBeforeOpenDialog();
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
            return String.format("%s;%s", path.get().getConverterFactory().getIdentifier(),
                    path.get().getConsumerFactory().getIdentifier());
        }

    }

    @Widget(title = "Type",
            description = "Columns that match the given KNIME type will be mapped to the specified database type.")
    @ArrayWidget(addButtonText = "Add type", elementTitle = "Type")
    @ValueReference(TypeMappingParameters.ByTypeRef.class)
    @Layout(TypeMappingParameters.MappingByType.class)
    @Modification(TypeMappingParameters.ByTypeModification.class)
    public ByTypeMappingSettings[] m_byTypeSettings = new ByTypeMappingSettings[0];

    private static final class ByTypeModification extends ByTypeMappingModification {

        @Override
        protected Optional<Class<? extends DataTypeChoicesProvider>> getFromColTypeChoicesProvider() {
            return Optional.of(TypeMappingParameters.ByTypeDynamicKnimeTypeChoicesProvider.class);
        }

        @Override
        protected Optional<Class<? extends StringChoicesProvider>> getToColTypeChoicesProvider() {
            return Optional.of(TypeMappingParameters.ToDBTypeByTypeChoicesProvider.class);
        }

        @Override
        protected Optional<Class<? extends StateProvider<String>>> getToColTypeValueProvider() {
            return Optional.of(TypeMappingParameters.ToTypeProvider.class);
        }

    }

    interface ByTypeRef extends ParameterReference<ByTypeMappingSettings[]> {
    }

    private static final class ByTypeDynamicKnimeTypeChoicesProvider extends DynamicKnimeTypeChoicesProvider {

        private Supplier<ByTypeMappingSettings[]> m_array;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_array = initializer.computeFromValueSupplier(TypeMappingParameters.ByTypeRef.class);
            super.init(initializer);
        }

        @Override
        protected ByTypeMappingSettings[] getByTypeOutputSettings() {
            return m_array.get();
        }

    }

    private static final class ToDBTypeByTypeChoicesProvider extends
            ToDBTypeChoicesProvider<ByTypeMappingSettings.FromColTypeRef> {
        ToDBTypeByTypeChoicesProvider() {
            super(ByTypeMappingSettings.FromColTypeRef.class);
        }
    }

    private static final class ToTypeProvider implements StateProvider<String> {

        private Supplier<DataType> m_fromType;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_fromType = initializer.computeFromValueSupplier(ByTypeMappingSettings.FromColTypeRef.class);
            initializer.computeBeforeOpenDialog();
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
            return String.format("%s;%s", path.get().getConverterFactory().getIdentifier(),
                    path.get().getConsumerFactory().getIdentifier());
        }

    }

}
