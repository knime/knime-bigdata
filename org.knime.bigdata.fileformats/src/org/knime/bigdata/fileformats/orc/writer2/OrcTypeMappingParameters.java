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

import org.apache.orc.TypeDescription;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCDestination;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCSource;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCTypeMappingService;
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
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.migration.Migration;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.widget.choices.ChoicesProvider;

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

    private static final DataTypeMappingService<TypeDescription, ORCSource, ORCDestination> MAPPING_SERVICE =
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

            group.find(ByNameMappingSettings.ToColTypeRef.class).modifyAnnotation(Widget.class)
                .withProperty("description", "ORC data type to map to.").modify();

            group.find(ByNameMappingSettings.ToColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByNameOrcTypeChoicesProvider.class).modify();

            group.find(ByNameMappingSettings.ToColTypeRef.class).addAnnotation(ValueProvider.class)
                .withValue(ByNameOrcTypeValueProvider.class).modify();
        }

        private static final class ByNameKnimeTypeChoicesProvider
            extends KnimeSourceTypeChoicesProvider<TypeDescription, ORCSource, ORCDestination> {

            ByNameKnimeTypeChoicesProvider() {
                super(MAPPING_SERVICE);
            }
        }

        private static final class ByNameOrcTypeChoicesProvider extends
            TypeChoicesProvider<TypeDescription, ORCSource, ORCDestination, ByNameMappingSettings.FromColTypeRef> {

            ByNameOrcTypeChoicesProvider() {
                super(MAPPING_SERVICE, ByNameMappingSettings.FromColTypeRef.class);
            }
        }

        private static final class ByNameOrcTypeValueProvider extends
            AbstractExternalTypeValueProvider<TypeDescription, ORCSource, ORCDestination, ByNameMappingSettings.FromColTypeRef> {

            ByNameOrcTypeValueProvider() {
                super(MAPPING_SERVICE, ByNameMappingSettings.FromColTypeRef.class);
            }
        }
    }

    static final class ByTypeModification implements Modification.Modifier {

        @Override
        public void modify(final Modification.WidgetGroupModifier group) {
            group.find(ByTypeMappingSettings.FromColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByTypeKnimeTypeChoicesProvider.class).modify();

            group.find(ByTypeMappingSettings.ToColTypeRef.class).modifyAnnotation(Widget.class)
                .withProperty("description", "ORC data type to map to.").modify();

            group.find(ByTypeMappingSettings.ToColTypeRef.class).addAnnotation(ChoicesProvider.class)
                .withValue(ByTypeOrcTypeChoicesProvider.class).modify();

            group.find(ByTypeMappingSettings.ToColTypeRef.class).addAnnotation(ValueProvider.class)
                .withValue(ByTypeOrcTypeValueProvider.class).modify();
        }

        private static final class ByTypeKnimeTypeChoicesProvider
            extends AvailableKnimeTypeChoicesProvider<TypeDescription, ORCSource, ORCDestination> {

            ByTypeKnimeTypeChoicesProvider() {
                super(MAPPING_SERVICE);
            }
        }

        private static final class ByTypeOrcTypeChoicesProvider extends
            TypeChoicesProvider<TypeDescription, ORCSource, ORCDestination, ByTypeMappingSettings.FromColTypeRef> {

            ByTypeOrcTypeChoicesProvider() {
                super(MAPPING_SERVICE, ByTypeMappingSettings.FromColTypeRef.class);
            }
        }

        private static final class ByTypeOrcTypeValueProvider extends
            AbstractExternalTypeValueProvider<TypeDescription, ORCSource, ORCDestination, ByTypeMappingSettings.FromColTypeRef> {

            ByTypeOrcTypeValueProvider() {
                super(MAPPING_SERVICE, ByTypeMappingSettings.FromColTypeRef.class);
            }
        }
    }

    static final class OrcTypeMappingPersistor
        extends TypeMappingPersistor<OrcTypeMappingParameters, TypeDescription, ORCSource, ORCDestination> {

        OrcTypeMappingPersistor() {
            super(MAPPING_SERVICE, OrcTypeMappingParameters::new);
        }
    }

    static final class OrcTypeMappingMigration extends TypeMappingMigration<OrcTypeMappingParameters> {
    }
}
