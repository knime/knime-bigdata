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

import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeDestination;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeMappingService;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetLogicalTypeSource;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.bigdata.fileformats.utility.ByNameMappingSettings;
import org.knime.bigdata.fileformats.utility.ByTypeMappingSettings;
import org.knime.bigdata.fileformats.utility.TypeMappingParameters;
import org.knime.bigdata.fileformats.utility.TypeMappingUtils.AbstractExternalTypeValueProvider;
import org.knime.bigdata.fileformats.utility.TypeMappingUtils.AvailableKnimeTypeChoicesProvider;
import org.knime.bigdata.fileformats.utility.TypeMappingUtils.KnimeSourceTypeChoicesProvider;
import org.knime.bigdata.fileformats.utility.TypeMappingUtils.TypeChoicesProvider;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.node.datatype.mapping.SettingsModelDataTypeMapping;
import org.knime.node.parameters.migration.Migration;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.widget.choices.ChoicesProvider;

/**
 * Node parameters for type mapping part in Parquet Writer. Backwards compatible to the settings structure of
 * {@link SettingsModelDataTypeMapping}. KNIME to External type mappings by name and type.
 *
 * @author Jochen Reißinger, TNG Technology Consulting GmbH
 */
@Persistor(ParquetTypeMappingParameters.ParquetTypeMappingPersistor.class)
@Migration(ParquetTypeMappingParameters.ParquetTypeMappingMigration.class)
@Modification({ParquetTypeMappingParameters.ByNameModification.class,
    ParquetTypeMappingParameters.ByTypeModification.class})
@SuppressWarnings("restriction")
final class ParquetTypeMappingParameters extends TypeMappingParameters {

    private static final DataTypeMappingService<ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination> MAPPING_SERVICE =
        ParquetLogicalTypeMappingService.getInstance();

    ParquetTypeMappingParameters() {
        // default constructor
    }

    ParquetTypeMappingParameters(final ByNameMappingSettings[] byNameSettings,
        final ByTypeMappingSettings[] byTypeSettings) {
        super(byNameSettings, byTypeSettings);
    }

    static final class ByNameModification implements Modification.Modifier {

        @Override
        public void modify(final Modification.WidgetGroupModifier group) {
            group.find(ByNameMappingSettings.FromColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByNameKnimeTypeChoicesProvider.class).modify();

            group.find(ByNameMappingSettings.ToColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByNameParquetTypeChoicesProvider.class).modify();

            group.find(ByNameMappingSettings.ToColTypeRef.class).addAnnotation(ValueProvider.class)
                .withValue(ByNameParquetTypeValueProvider.class).modify();
        }

        private static final class ByNameKnimeTypeChoicesProvider extends
            KnimeSourceTypeChoicesProvider<ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination> {

            ByNameKnimeTypeChoicesProvider() {
                super(MAPPING_SERVICE);
            }
        }

        private static final class ByNameParquetTypeChoicesProvider extends
            TypeChoicesProvider<ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination, ByNameMappingSettings.FromColTypeRef> {

            ByNameParquetTypeChoicesProvider() {
                super(MAPPING_SERVICE, ByNameMappingSettings.FromColTypeRef.class);
            }
        }

        private static final class ByNameParquetTypeValueProvider extends
            AbstractExternalTypeValueProvider<ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination, ByNameMappingSettings.FromColTypeRef> {

            ByNameParquetTypeValueProvider() {
                super(MAPPING_SERVICE, ByNameMappingSettings.FromColTypeRef.class);
            }
        }
    }

    static final class ByTypeModification implements Modification.Modifier {

        @Override
        public void modify(final Modification.WidgetGroupModifier group) {
            group.find(ByTypeMappingSettings.FromColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByTypeKnimeTypeChoicesProvider.class).modify();

            group.find(ByTypeMappingSettings.ToColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByTypeParquetTypeChoicesProvider.class).modify();

            group.find(ByTypeMappingSettings.ToColTypeRef.class).addAnnotation(ValueProvider.class)
                .withValue(ByTypeParquetTypeValueProvider.class).modify();
        }

        private static final class ByTypeKnimeTypeChoicesProvider extends
            AvailableKnimeTypeChoicesProvider<ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination> {

            ByTypeKnimeTypeChoicesProvider() {
                super(MAPPING_SERVICE);
            }
        }

        private static final class ByTypeParquetTypeChoicesProvider extends
            TypeChoicesProvider<ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination, ByTypeMappingSettings.FromColTypeRef> {

            ByTypeParquetTypeChoicesProvider() {
                super(MAPPING_SERVICE, ByTypeMappingSettings.FromColTypeRef.class);
            }
        }

        private static final class ByTypeParquetTypeValueProvider extends
            AbstractExternalTypeValueProvider<ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination, ByTypeMappingSettings.FromColTypeRef> {

            ByTypeParquetTypeValueProvider() {
                super(MAPPING_SERVICE, ByTypeMappingSettings.FromColTypeRef.class);
            }
        }
    }

    static final class ParquetTypeMappingPersistor extends
        TypeMappingPersistor<ParquetTypeMappingParameters, ParquetType, ParquetLogicalTypeSource, ParquetLogicalTypeDestination> {

        ParquetTypeMappingPersistor() {
            super(MAPPING_SERVICE, ParquetTypeMappingParameters::new);
        }
    }

    static final class ParquetTypeMappingMigration extends TypeMappingMigration<ParquetTypeMappingParameters> {
    }
}
