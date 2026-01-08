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

import java.util.Optional;
import java.util.function.Supplier;

import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeMappingService;
import org.knime.bigdata.fileformats.parquet.writer3.DBOutputTypeMapping.ByNameOutputMappingSettings;
import org.knime.bigdata.fileformats.parquet.writer3.DBOutputTypeMapping.ByNameOutputMappingSettings.ByNameOutputMappingModification;
import org.knime.bigdata.fileformats.parquet.writer3.DBOutputTypeMapping.ByTypeOutputMappingSettings;
import org.knime.bigdata.fileformats.parquet.writer3.DBOutputTypeMapping.ByTypeOutputMappingSettings.ByTypeOutputMappingModification;
import org.knime.bigdata.fileformats.parquet.writer3.DBOutputTypeMapping.ByTypeOutputMappingSettings.DynamicKnimeTypeChoicesProvider;
import org.knime.bigdata.fileformats.parquet.writer3.DBOutputTypeMapping.KnimeTypeChoicesProvider;
import org.knime.bigdata.fileformats.parquet.writer3.DBTypeMappingUtils.ToDBTypeChoicesProvider;
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
 * Settings for output column type mapping used to overwrite default mappings.
 *
 * These are currently used by all manipulator nodes and with that hard-coded to use the second input port.
 *
 * @author Paul Baernreuther
 */
@SuppressWarnings("javadoc")
public final class DBOutputTypeMappingParameters implements NodeParameters {

    @Section(title = "Output mapping by name (Excluding Defaults)")
    interface MappingByName {
    }

    @Section(title = "Output mapping by type (Excluding Defaults)")
    @After(DBOutputTypeMappingParameters.MappingByName.class)
    interface MappingByType {
    }

    /**
     * Output mapping settings by name.
     */
    @Widget(title = "Name", description = """
            Columns that match the given name (or regular expression) and KNIME type will be mapped to the \
            specified database type.
            """)
    @ArrayWidget(addButtonText = "Add name", elementTitle = "Column name")
    @Layout(DBOutputTypeMappingParameters.MappingByName.class)
    @Modification(DBOutputTypeMappingParameters.ByNameModification.class)
    public ByNameOutputMappingSettings[] m_byNameSettings = new ByNameOutputMappingSettings[0];

    static final class ByNameModification extends ByNameOutputMappingModification {

        @Override
        protected Optional<Class<? extends DataTypeChoicesProvider>> getFromColTypeChoicesProvider() {
            return Optional.of(DBOutputTypeMappingParameters.ByNameKnimeTypeChoicesProvider.class);
        }

        @Override
        protected Optional<Class<? extends StringChoicesProvider>> getToColTypeChoicesProvider() {
            return Optional.of(DBOutputTypeMappingParameters.ToDBTypeByNameChoicesProvider.class);
        }

        @Override
        protected Optional<Class<? extends StateProvider<String>>> getToColTypeValueProvider() {
            return Optional.of(DBOutputTypeMappingParameters.ToNameProvider.class);
        }

    }

    static final class ByNameKnimeTypeChoicesProvider extends KnimeTypeChoicesProvider {
        // Inherits init() and choices() from KnimeTypeChoicesProvider
    }

    abstract static class AbstractToDBTypeChoicesProvider<R extends ParameterReference<DataType>>
            extends ToDBTypeChoicesProvider<R> {

        protected AbstractToDBTypeChoicesProvider(final Class<R> ref) {
            super(ref);
        }
    }

    static final class ToDBTypeByNameChoicesProvider extends
        DBOutputTypeMappingParameters.AbstractToDBTypeChoicesProvider<ByNameOutputMappingSettings.FromColTypeRef> {

        ToDBTypeByNameChoicesProvider() {
            super(ByNameOutputMappingSettings.FromColTypeRef.class);
        }
    }

    static final class ToNameProvider implements StateProvider<String> {

        private Supplier<String> m_fromColumn;

        private Supplier<DataType> m_fromType;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_fromColumn = initializer.computeFromValueSupplier(ByNameOutputMappingSettings.FromColRef.class);
            m_fromType = initializer.computeFromValueSupplier(ByNameOutputMappingSettings.FromColTypeRef.class);
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

    /**
     * Output mapping settings by type.
     */
    @Widget(title = "Type",
        description = "Columns that match the given KNIME type will be mapped to the specified database type.")
    @ArrayWidget(addButtonText = "Add type", elementTitle = "Type")
    @ValueReference(DBOutputTypeMappingParameters.ByTypeRef.class)
    @Layout(DBOutputTypeMappingParameters.MappingByType.class)
    @Modification(DBOutputTypeMappingParameters.ByTypeModification.class)
    public ByTypeOutputMappingSettings[] m_byTypeSettings = new ByTypeOutputMappingSettings[0];

    static final class ByTypeModification extends ByTypeOutputMappingModification {

        @Override
        protected Optional<Class<? extends DataTypeChoicesProvider>> getFromColTypeChoicesProvider() {
            return Optional.of(DBOutputTypeMappingParameters.ByTypeDynamicKnimeTypeChoicesProvider.class);
        }

        @Override
        protected Optional<Class<? extends StringChoicesProvider>> getToColTypeChoicesProvider() {
            return Optional.of(DBOutputTypeMappingParameters.ToDBTypeByTypeChoicesProvider.class);
        }

        @Override
        protected Optional<Class<? extends StateProvider<String>>> getToColTypeValueProvider() {
            return Optional.of(DBOutputTypeMappingParameters.ToTypeProvider.class);
        }

    }

    interface ByTypeRef extends ParameterReference<ByTypeOutputMappingSettings[]> {}

    static final class ByTypeDynamicKnimeTypeChoicesProvider extends DynamicKnimeTypeChoicesProvider{

        private Supplier<ByTypeOutputMappingSettings[]> m_array;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_array = initializer.computeFromValueSupplier(DBOutputTypeMappingParameters.ByTypeRef.class);
            super.init(initializer);
        }

        @Override
        protected ByTypeOutputMappingSettings[] getByTypeOutputSettings() {
            return m_array.get();
        }

    }

    static final class ToDBTypeByTypeChoicesProvider extends
        DBOutputTypeMappingParameters.AbstractToDBTypeChoicesProvider<ByTypeOutputMappingSettings.FromColTypeRef> {
        ToDBTypeByTypeChoicesProvider() {
            super(ByTypeOutputMappingSettings.FromColTypeRef.class);
        }
    }

    static final class ToTypeProvider implements StateProvider<String> {

        private Supplier<DataType> m_fromType;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_fromType = initializer.computeFromValueSupplier(ByTypeOutputMappingSettings.FromColTypeRef.class);
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
