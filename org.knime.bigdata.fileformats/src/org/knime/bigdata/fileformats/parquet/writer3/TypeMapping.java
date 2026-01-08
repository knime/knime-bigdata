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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.core.data.DataType;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification.WidgetGroupModifier;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeMappingService;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMappingUtils.ConsumptionPathPersistor;
import org.knime.bigdata.fileformats.parquet.writer3.TypeMappingUtils.FilterType;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.WidgetGroup;
import org.knime.node.parameters.persistence.Persistable;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.DataTypeChoicesProvider;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.text.TextInputWidget;

/**
 *
 * @author Jochen Reißinger, TNG Technology Consulting GmbH
 */
@SuppressWarnings({"restriction", "javadoc"})
public final class TypeMapping {

    private TypeMapping() {
        // Utility class
    }

    interface MappingSettings extends WidgetGroup, Persistable {
        /**
         * @return the fromType
         */
        DataType getFromType();

        /**
         * @return the toType
         */
        String getToType();

        /**
         * Converts the string representation of a consumption path to the actual {@link ConsumptionPath}.
         *
         * @param mappingService
         * @return the consumption path
         * @throws InvalidSettingsException if consumption path is not found
         */
        default ConsumptionPath getConsumptionPath() throws InvalidSettingsException {
            final var mappingService = ParquetLogicalTypeMappingService.getInstance();
            return mappingService.getConsumptionPathsFor(getFromType()).stream()
                .filter(cpath -> String.format("%s;%s", cpath.getConverterFactory().getIdentifier(),
                    cpath.getConsumerFactory().getIdentifier()).equals(getToType()))
                .findFirst().orElseThrow(
                    () -> new InvalidSettingsException(String.format("No consumption path found for %s", getToType())));
        }
    }

    public static final class ByNameMappingSettings implements MappingSettings {

        ByNameMappingSettings() {

        }

        public ByNameMappingSettings(final FilterType filterType, final String fromColName,
            final DataType fromColType, final String toColType) {
            this.m_filterType = filterType;
            this.m_fromColName = fromColName;
            this.m_fromColType = fromColType;
            this.m_toColType = toColType;
        }

        @Widget(title = "Column selection type",
            description = "The option allows you to select how the column is matched.")
        @ValueSwitchWidget
        FilterType m_filterType = FilterType.MANUAL;

        @Widget(title = "Column name", description = "The column name or regex expression.")
        @TextInputWidget
        @ValueReference(FromColRef.class)
        String m_fromColName = "";

        @Widget(title = "Source type", description = "Datatype to map from.")
        @ValueReference(ByNameMappingSettings.FromColTypeRef.class)
        @Modification.WidgetReference(FromColTypeRef.class)
        DataType m_fromColType;

        @Widget(title = "Mapping", description = "Datatype to map to.")
        @Persistor(ConsumptionPathPersistor.class)
        @Modification.WidgetReference(ToColTypeRef.class)
        String m_toColType;

        @SuppressWarnings("javadoc")
        public interface FromColRef extends ParameterReference<String> {
        }

        @SuppressWarnings("javadoc")
        public interface FromColTypeRef extends ParameterReference<DataType>, Modification.Reference {
        }

        @SuppressWarnings("javadoc")
        public interface ToColTypeRef extends Modification.Reference {

        }

        public FilterType getFilterType() {
            return m_filterType;
        }

        public String getFromColName() {
            return m_fromColName;
        }

        @Override
        public DataType getFromType() {
            return m_fromColType;
        }

        @Override
        public String getToType() {
            return m_toColType;
        }

        public abstract static class ByNameMappingModification implements Modification.Modifier {

            @Override
            public void modify(final WidgetGroupModifier group) {
                final var fromChoices = getFromColTypeChoicesProvider();
                if (fromChoices.isPresent()) {
                    group.find(FromColTypeRef.class).addAnnotation(ChoicesProvider.class).withValue(fromChoices.get())
                        .modify();
                }
                final var toChoices = getToColTypeChoicesProvider();
                if (toChoices.isPresent()) {
                    group.find(ToColTypeRef.class).addAnnotation(ChoicesProvider.class).withValue(toChoices.get())
                        .modify();
                }
                final var toValue = getToColTypeValueProvider();
                if (toValue.isPresent()) {
                    group.find(ToColTypeRef.class).addAnnotation(ValueProvider.class).withValue(toValue.get()).modify();
                }
            }

            protected abstract Optional<Class<? extends DataTypeChoicesProvider>> getFromColTypeChoicesProvider();

            protected abstract Optional<Class<? extends StringChoicesProvider>> getToColTypeChoicesProvider();

            protected abstract Optional<Class<? extends StateProvider<String>>> getToColTypeValueProvider();

        }

    }

    public static final class ByTypeMappingSettings implements MappingSettings {

        ByTypeMappingSettings() {

        }

        public ByTypeMappingSettings(final DataType fromType, final String toType) {
            this.m_fromType = fromType;
            this.m_toType = toType;
        }

        @Widget(title = "KNIME type", description = "Datatype to map from.")
        @ValueReference(ByTypeMappingSettings.FromColTypeRef.class)
        @Modification.WidgetReference(FromColTypeRef.class)
        DataType m_fromType;

        @Widget(title = "Mapping", description = "Datatype to map to.")
        @Persistor(ConsumptionPathPersistor.class)
        @ValueReference(ToColTypeRef.class)
        @Modification.WidgetReference(ToColTypeRef.class)
        String m_toType;

        @SuppressWarnings("javadoc")
        public interface FromColTypeRef extends ParameterReference<DataType>, Modification.Reference {
        }

        @SuppressWarnings("javadoc")
        public interface ToColTypeRef extends ParameterReference<String>, Modification.Reference {
        }

        @Override
        public DataType getFromType() {
            return m_fromType;
        }

        @Override
        public String getToType() {
            return m_toType;
        }

        public abstract static class ByTypeMappingModification implements Modification.Modifier {

            @Override
            public void modify(final WidgetGroupModifier group) {
                final var fromChoices = getFromColTypeChoicesProvider();
                if (fromChoices.isPresent()) {
                    group.find(FromColTypeRef.class).addAnnotation(ChoicesProvider.class).withValue(fromChoices.get())
                        .modify();
                }
                final var toChoices = getToColTypeChoicesProvider();
                if (toChoices.isPresent()) {
                    group.find(ToColTypeRef.class).addAnnotation(ChoicesProvider.class).withValue(toChoices.get())
                        .modify();
                }
                final var toValue = getToColTypeValueProvider();
                if (toValue.isPresent()) {
                    group.find(ToColTypeRef.class).addAnnotation(ValueProvider.class).withValue(toValue.get()).modify();
                }
            }

            protected abstract Optional<Class<? extends DataTypeChoicesProvider>> getFromColTypeChoicesProvider();

            protected abstract Optional<Class<? extends StringChoicesProvider>> getToColTypeChoicesProvider();

            protected abstract Optional<Class<? extends StateProvider<String>>> getToColTypeValueProvider();

        }

        public abstract static class DynamicKnimeTypeChoicesProvider implements DataTypeChoicesProvider {

            private Supplier<DataType> m_fromType;

            @Override
            public void init(final StateProviderInitializer initializer) {
                this.m_fromType = initializer.computeFromValueSupplier(FromColTypeRef.class);
                initializer.computeBeforeOpenDialog();
            }

            @Override
            public List<DataType> choices(final NodeParametersInput context) {
                final var mappingService = ParquetLogicalTypeMappingService.getInstance();
                final var existingTypes = Arrays.stream(getByTypeOutputSettings())
                    .map(setting -> setting.m_fromType)
                    .filter(type -> type != null && !type.equals(this.m_fromType.get()))
                    .collect(Collectors.toSet());
                return mappingService.getKnimeSourceTypes().stream()
                    .filter(type -> !existingTypes.contains(type))
                    .sorted((t1, t2) -> t1.toPrettyString().compareTo(t2.toPrettyString()))
                    .toList();
            }

            protected abstract ByTypeMappingSettings[] getByTypeOutputSettings();
        }

    }
}
