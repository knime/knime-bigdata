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

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeMappingService;
import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.StringChoice;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;

/**
 * Contains utility methods for {@link TypeMappingSettings} and {@link TypeMappingParameters}.
 *
 * @author Jochen Reißinger, TNG Technology Consulting GmbH
 */
public final class TypeMappingUtils {

    enum FilterType {

            @Label(value = "Manual", description = "Use the exact name of the column")
            MANUAL,

            @Label(value = "Regex", description = "Allow regex expressions to select multiple columns")
            REGEX
    }

    abstract static class ParquetTypeChoicesProvider<T extends ParameterReference<DataType>>
        implements StringChoicesProvider {

        private final Class<T> m_ref;

        private Supplier<DataType> m_fromColType;

        protected ParquetTypeChoicesProvider(final Class<T> ref) {
            this.m_ref = ref;
        }

        @Override
        public void init(final StateProviderInitializer initializer) {
            this.m_fromColType = initializer.computeFromValueSupplier(this.m_ref);
            initializer.computeBeforeOpenDialog();
        }

        @Override
        public List<StringChoice> computeState(final NodeParametersInput context) {
            final DataType dataType = this.m_fromColType.get();
            if (dataType == null) {
                return List.of();
            }
            final var mappingService = ParquetLogicalTypeMappingService.getInstance();
            return mappingService.getConsumptionPathsFor(dataType).stream()
                .sorted(Comparator.comparing(ConsumptionPath::toString))
                .map(path -> new StringChoice(getIdForConsumptionPath(path),
                    "\u2192 " + path.getConverterFactory().getDestinationType().getSimpleName() + " \u2192 "
                        + path.getConsumerFactory().getDestinationType().toString()))
                .toList();
        }
    }

    static String getIdForConsumptionPath(final ConsumptionPath path) {
        return formatStringPair(path.getConverterFactory().getIdentifier(), path.getConsumerFactory().getIdentifier());
    }

    private static String formatStringPair(final String first, final String second) {
        return first + ";" + second;
    }
}
