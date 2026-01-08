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
 *   Jun 25, 2025 (Martin Sillye, TNG Technology Consulting GmbH): created
 */
package org.knime.bigdata.fileformats.parquet.writer3;

import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeDestination;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeMappingService;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeSource;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.StringChoice;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;

/**
 *
 * @author Martin Sillye, TNG Technology Consulting GmbH
 */
@SuppressWarnings("restriction")
public final class TypeMappingUtils {

    private TypeMappingUtils() {
        // Utils class
    }

    /**
     *
     * @author Martin Sillye, TNG Technology Consulting GmbH
     */
    public enum FilterType {
            /**
             * Manual filter.
             */
            @Label(value = "Manual", description = "Use the exact name of the column")
            MANUAL, //
            /**
             * Regex filter.
             */
            @Label(value = "Regex", description = "Allow regex expressions to select multiple columns")
            REGEX
    }

    abstract static class ToTypeChoicesProvider<T1, T2 extends ParameterReference<T1>>
        implements StringChoicesProvider {

        ToTypeChoicesProvider(final Class<T2> ref) {
            this.m_ref = ref;
        }

        private Supplier<T1> m_fromColType;

        private Class<T2> m_ref;

        @Override
        public void init(final StateProviderInitializer initializer) {
            this.m_fromColType = initializer.computeFromValueSupplier(this.m_ref);
            initializer.computeBeforeOpenDialog();
        }

        @Override
        public List<StringChoice> computeState(final NodeParametersInput context) {
            if (this.m_fromColType.get() == null) {
                return List.of();
            }
            final var mappingService = ParquetLogicalTypeMappingService.getInstance();
            return getChoices(mappingService, this.m_fromColType.get());
        }

        protected abstract List<StringChoice> getChoices(
            DataTypeMappingService<ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination> mappingService, T1 fromColType);
    }

    abstract static class ToKnimeTypeChoicesProvider<T extends ParameterReference<String>>
        extends ToTypeChoicesProvider<String, T> {

        ToKnimeTypeChoicesProvider(final Class<T> ref) {
            super(ref);
        }

        @Override
        protected List<StringChoice> getChoices(
            final DataTypeMappingService<ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination>
            mappingService,
            final String fromColType) {
            final ParquetType parquetType = mappingService.convertStringToExternalType(fromColType);

            return mappingService.getProductionPathsFor(parquetType).stream()
                .sorted((p1, p2) -> p1.toString().compareTo(p2.toString()))
                .map(path -> new StringChoice(
                    String.format("%s;%s", path.getProducerFactory().getIdentifier(),
                        path.getConverterFactory().getIdentifier()),
                    "\u2192 " + path.getConverterFactory().getSourceType().getSimpleName() + " \u2192 "
                        + path.getDestinationType().getName()))
                .toList();
        }
    }

    public abstract static class ToDBTypeChoicesProvider<T extends ParameterReference<DataType>>
        extends ToTypeChoicesProvider<DataType, T> {

        /**
         * @param ref {@link ParameterReference} class for the From type field.
         */
        protected ToDBTypeChoicesProvider(final Class<T> ref) {
            super(ref);
        }

        @Override
        protected List<StringChoice> getChoices(final DataTypeMappingService<ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination> mappingService,
            final DataType fromColType) {
            return mappingService.getConsumptionPathsFor(fromColType).stream()
                .sorted((p1, p2) -> p1.toString().compareTo(p2.toString()))
                .map(path -> new StringChoice(
                    String.format("%s;%s", path.getConverterFactory().getIdentifier(),
                        path.getConsumerFactory().getIdentifier()),
                    "\u2192 " + path.getConverterFactory().getDestinationType().getSimpleName() + " \u2192 "
                        + path.getConsumerFactory().getDestinationType().toString()))
                .toList();
        }
    }

    abstract static class PathPersistor implements NodeParametersPersistor<String> {

        PathPersistor(final String fromKey, final String toKey) {
            this.m_fromKey = fromKey;
            this.m_toKey = toKey;
        }

        final String m_fromKey;

        final String m_toKey;

        @Override
        public String load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final var from = settings.getString(m_fromKey, "");
            final var to = settings.getString(m_toKey, "");
            if (StringUtils.isBlank(from) && StringUtils.isBlank(to)) {
                return "";
            }
            return String.format("%s;%s", from, to);
        }

        @Override
        public void save(final String obj, final NodeSettingsWO settings) {
            if (obj != null && obj.contains(";")) {
                final var split = obj.split(";");
                settings.addString(m_fromKey, split[0]);
                settings.addString(m_toKey, split[1]);
            }
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{new String[]{m_fromKey}, new String[]{m_toKey}};
        }
    }
    /**
     *
     * @author Martin Sillye, TNG Technology Consulting GmbH
     */
    public static final class ConsumptionPathPersistor extends PathPersistor {

        ConsumptionPathPersistor() {
            super(CONVERTER_FACTORY, CONSUMER_FACTORY);
        }

        static final String CONSUMER_FACTORY = "consumption_path_consumer";

        static final String CONVERTER_FACTORY = "consumption_path_converter";
    }
}
