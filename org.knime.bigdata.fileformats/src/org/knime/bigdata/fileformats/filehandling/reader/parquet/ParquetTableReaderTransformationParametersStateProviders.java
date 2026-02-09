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
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet;

import java.util.function.Supplier;

import org.knime.base.node.io.filehandling.webui.reader2.TransformationParametersStateProviders;
import org.knime.bigdata.fileformats.filehandling.reader.BigDataMultiTableReadConfig;
import org.knime.bigdata.fileformats.filehandling.reader.BigDataReaderConfig;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.ParquetTableReaderNodeParameters.ParquetReaderParametersRef;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.ParquetTableReaderSpecific.ConfigAndReader;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.ParquetTableReaderSpecific.ProductionPathProviderAndTypeHierarchy;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeTypeSerializer;

/**
 * @author Robin Gerling, KNIME GmbH, Konstanz, Germany
 */
final class ParquetTableReaderTransformationParametersStateProviders {

    static final class TypedReaderTableSpecsProvider
        extends TransformationParametersStateProviders.TypedReaderTableSpecsProvider<//
                BigDataReaderConfig, KnimeType, BigDataMultiTableReadConfig>
        implements ConfigAndReader {

        private Supplier<ParquetTableReaderParameters> m_readerNodeParamsSupplier;

        @Override
        protected void applyParametersToConfig(final BigDataMultiTableReadConfig config) {
            m_readerNodeParamsSupplier.get().saveToConfig(config);
        }

        @Override
        public void init(final StateProviderInitializer initializer) {
            super.init(initializer);
            m_readerNodeParamsSupplier = initializer.getValueSupplier(ParquetReaderParametersRef.class);
        }

        interface Dependent
            extends TransformationParametersStateProviders.TypedReaderTableSpecsProvider.Dependent<KnimeType> {
            @SuppressWarnings("unchecked")
            @Override
            default Class<TypedReaderTableSpecsProvider> getTypedReaderTableSpecsProvider() {
                return TypedReaderTableSpecsProvider.class;
            }

            @Override
            default void initConfigIdTriggers(final StateProviderInitializer initializer) {
                // No parameters affect table spec detection
            }
        }
    }

    static final class TableSpecSettingsProvider
        extends TransformationParametersStateProviders.TableSpecSettingsProvider<KnimeType>
        implements TypedReaderTableSpecsProvider.Dependent, KnimeTypeSerializer {
    }

    static final class TransformationElementSettingsProvider
        extends TransformationParametersStateProviders.TransformationElementSettingsProvider<KnimeType> implements
        ProductionPathProviderAndTypeHierarchy, TypedReaderTableSpecsProvider.Dependent, KnimeTypeSerializer {

        @Override
        protected boolean hasMultipleFileHandling() {
            return true;
        }
    }

    static final class TypeChoicesProvider extends TransformationParametersStateProviders.TypeChoicesProvider<KnimeType>
        implements ProductionPathProviderAndTypeHierarchy, TypedReaderTableSpecsProvider.Dependent {
    }

    static final class TransformationSettingsWidgetModification
        extends TransformationParametersStateProviders.TransformationSettingsWidgetModification<KnimeType> {

        @Override
        protected Class<TableSpecSettingsProvider> getSpecsValueProvider() {
            return TableSpecSettingsProvider.class;
        }

        @Override
        protected Class<TypeChoicesProvider> getTypeChoicesProvider() {
            return TypeChoicesProvider.class;
        }

        @Override
        protected Class<TransformationElementSettingsProvider> getTransformationSettingsValueProvider() {
            return TransformationElementSettingsProvider.class;
        }

    }

    private ParquetTableReaderTransformationParametersStateProviders() {
        // Not intended to be initialized
    }

}
