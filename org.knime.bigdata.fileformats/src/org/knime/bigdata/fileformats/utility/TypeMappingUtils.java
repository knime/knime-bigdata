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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.data.convert.map.Destination;
import org.knime.core.data.convert.map.Source;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.widget.choices.DataTypeChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.StringChoice;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;

/**
 * Contains utility methods and reusable abstract provider classes for type mapping widgets used in file format writer
 * nodes.
 *
 * @author Jochen Reißinger, TNG Technology Consulting GmbH
 */
@SuppressWarnings({"javadoc", "restriction"})
public final class TypeMappingUtils {

    public enum FilterType {

            @Label(value = "Manual", description = "Use the exact name of the column")
            MANUAL,

            @Label(value = "Regex", description = "Allow regex expressions to select multiple columns")
            REGEX
    }

    /**
     * Abstract base class for choices providers that map a KNIME {@link DataType} to a list of available external
     * target types for a given format.
     *
     * @param <X> the external type whose instances describe the target data types
     * @param <S> the external source type, must implement {@link Source}{@code <X>}
     * @param <D> the external destination type, must implement {@link Destination}{@code <X>}
     * @param <T> the parameter reference type used to track the selected source {@link DataType}
     */
    public abstract static class TypeChoicesProvider<X, S extends Source<X>, D extends Destination<X>, T extends ParameterReference<DataType>>
        implements StringChoicesProvider {

        private final Class<T> m_ref;

        private final DataTypeMappingService<X, S, D> m_mappingService;

        private Supplier<DataType> m_fromColType;

        /**
         * @param mappingService the format-specific mapping service
         * @param ref the parameter reference class used to track the selected source {@link DataType}
         */
        protected TypeChoicesProvider(final DataTypeMappingService<X, S, D> mappingService, final Class<T> ref) {
            this.m_mappingService = mappingService;
            this.m_ref = ref;
        }

        @Override
        public void init(final StateProviderInitializer initializer) {
            this.m_fromColType = initializer.computeFromValueSupplier(this.m_ref);
            initializer.computeAfterOpenDialog();
        }

        @Override
        public List<StringChoice> computeState(final NodeParametersInput context) {
            final DataType dataType = this.m_fromColType.get();
            if (dataType == null) {
                return List.of();
            }
            return m_mappingService.getConsumptionPathsFor(dataType).stream()
                .sorted(Comparator.comparing(ConsumptionPath::toString))
                .map(path -> new StringChoice(getIdForConsumptionPath(path),
                    "\u2192 " + path.getConverterFactory().getDestinationType().getSimpleName() + " \u2192 "
                        + path.getConsumerFactory().getDestinationType().toString()))
                .toList();
        }
    }

    public static String getIdForConsumptionPath(final ConsumptionPath path) {
        return path.getConverterFactory().getIdentifier() + ";" + path.getConsumerFactory().getIdentifier();
    }

    /**
     * Abstract base for a {@link DataTypeChoicesProvider} that lists all KNIME source types known to the mapping
     * service, sorted alphabetically.
     *
     * @param <X> the external type
     * @param <S> the external source type
     * @param <D> the external destination type
     */
    public abstract static class KnimeSourceTypeChoicesProvider<X, S extends Source<X>, D extends Destination<X>>
        implements DataTypeChoicesProvider {

        private final DataTypeMappingService<X, S, D> m_mappingService;

        /**
         * @param mappingService the format-specific mapping service
         */
        protected KnimeSourceTypeChoicesProvider(final DataTypeMappingService<X, S, D> mappingService) {
            this.m_mappingService = mappingService;
        }

        @Override
        public List<DataType> choices(final NodeParametersInput context) {
            return m_mappingService.getKnimeSourceTypes().stream()
                .sorted(Comparator.comparing(DataType::toPrettyString)).toList();
        }
    }

    /**
     * Abstract base for a {@link DataTypeChoicesProvider} that lists KNIME source types available for a new by-type
     * mapping row, excluding types that are already mapped in other rows.
     *
     * @param <X> the external type
     * @param <S> the external source type
     * @param <D> the external destination type
     */
    public abstract static class AvailableKnimeTypeChoicesProvider<X, S extends Source<X>, D extends Destination<X>>
        implements DataTypeChoicesProvider {

        private final DataTypeMappingService<X, S, D> m_mappingService;

        private Supplier<DataType> m_fromType;

        private Supplier<ByTypeMappingSettings[]> m_array;

        /**
         * @param mappingService the format-specific mapping service
         */
        protected AvailableKnimeTypeChoicesProvider(final DataTypeMappingService<X, S, D> mappingService) {
            this.m_mappingService = mappingService;
        }

        @Override
        public void init(final StateProviderInitializer initializer) {
            this.m_fromType = initializer.computeFromValueSupplier(ByTypeMappingSettings.FromColTypeRef.class);
            this.m_array = initializer.computeFromValueSupplier(TypeMappingParameters.ByTypeRef.class);
            initializer.computeBeforeOpenDialog();
        }

        @Override
        public List<DataType> choices(final NodeParametersInput context) {
            final var existingTypes = Arrays.stream(m_array.get()).map(s -> s.m_fromType)
                .filter(type -> type != null && !type.equals(this.m_fromType.get())).collect(Collectors.toSet());
            return m_mappingService.getKnimeSourceTypes().stream().filter(type -> !existingTypes.contains(type))
                .sorted(Comparator.comparing(DataType::toPrettyString)).toList();
        }
    }

    /**
     * Abstract base for a {@link StateProvider} that computes the default external type string for a selected KNIME
     * type, picking the first available consumption path.
     *
     * @param <X> the external type
     * @param <S> the external source type
     * @param <D> the external destination type
     * @param <T> the parameter reference type used to read the selected source {@link DataType}
     */
    public abstract static class AbstractExternalTypeValueProvider<X, S extends Source<X>, D extends Destination<X>, T extends ParameterReference<DataType>>
        implements StateProvider<String> {

        private final DataTypeMappingService<X, S, D> m_mappingService;

        private Supplier<DataType> m_fromType;

        private final Class<T> m_ref;

        /**
         * @param mappingService the format-specific mapping service
         * @param ref the parameter reference class used to read the selected source {@link DataType}
         */
        protected AbstractExternalTypeValueProvider(final DataTypeMappingService<X, S, D> mappingService,
            final Class<T> ref) {
            this.m_mappingService = mappingService;
            this.m_ref = ref;
        }

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_fromType = initializer.computeFromValueSupplier(m_ref);
        }

        @Override
        public String computeState(final NodeParametersInput context) {
            final DataType dataType = m_fromType.get();
            if (dataType == null) {
                return "";
            }
            return m_mappingService.getConsumptionPathsFor(dataType).stream().findFirst()
                .map(TypeMappingUtils::getIdForConsumptionPath).orElse("");
        }
    }

}
