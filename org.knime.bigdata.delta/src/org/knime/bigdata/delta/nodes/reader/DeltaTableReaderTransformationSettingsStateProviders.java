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
 *   May 15, 2024 (marcbux): created
 */
package org.knime.bigdata.delta.nodes.reader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.knime.base.node.io.filehandling.webui.FileChooserPathAccessor;
import org.knime.base.node.io.filehandling.webui.FileSystemPortConnectionUtil;
import org.knime.base.node.io.filehandling.webui.reader.CommonReaderTransformationSettings;
import org.knime.base.node.io.filehandling.webui.reader.CommonReaderTransformationSettingsStateProviders;
import org.knime.base.node.io.filehandling.webui.reader.CommonReaderTransformationSettingsStateProviders.FSLocationsProvider;
import org.knime.bigdata.delta.nodes.reader.DeltaTableReaderSpecific.ConfigAndReader;
import org.knime.bigdata.delta.nodes.reader.DeltaTableReaderSpecific.ProductionPathProviderAndTypeHierarchy;
import org.knime.bigdata.delta.nodes.reader.DeltaTableReaderTransformationSettings.ConfigIdSettings;
import org.knime.bigdata.delta.types.DeltaTableDataType;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification.WidgetGroupModifier;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocation;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderTableSpec;
import org.knime.filehandling.core.util.WorkflowContextUtil;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.updates.ValueProvider;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
@Modification(DeltaTableReaderTransformationSettingsStateProviders.TransformationSettingsWidgetModification.class)
final class DeltaTableReaderTransformationSettingsStateProviders {

    static final NodeLogger LOGGER = NodeLogger.getLogger(DeltaTableReaderTransformationSettingsStateProviders.class);

    static final class TypedReaderTableSpecsProvider
        extends CommonReaderTransformationSettingsStateProviders.TypedReaderTableSpecsProvider<//
            DeltaTableReaderConfig, ConfigIdSettings, DeltaTableDataType>
        implements ConfigAndReader {

        interface Dependent
            extends CommonReaderTransformationSettingsStateProviders.TypedReaderTableSpecsProvider.Dependent //
            <DeltaTableDataType> {

            @Override
            default Class<TypedReaderTableSpecsProvider> getTypedReaderTableSpecsProvider() {
                return TypedReaderTableSpecsProvider.class;
            }
        }

        @Override
        protected TypeReference<ConfigIdSettings> getConfigIdTypeReference() {
            return new TypeReference<>() {
            };
        }

        // Modified version using a single path
        @Override
        public Map<String, TypedReaderTableSpec<DeltaTableDataType>>
            computeState(final NodeParametersInput context) throws StateComputationFailureException {

            final var fileSelection = m_fileSelectionSupplier.get();
            if (!WorkflowContextUtil.hasWorkflowContext() // no workflow context available
                // no file selected (yet)
                || fileSelection.getFSLocation().equals(new FSLocation(FSCategory.LOCAL, ""))) {

                return computeStateFromPaths(Collections.emptyList());
            }

            var fsConnection = FileSystemPortConnectionUtil.getFileSystemConnection(context);
            if ((fileSelection.getFSLocation().getFSCategory() == FSCategory.CONNECTED) && fsConnection.isEmpty()) {
                return computeStateFromPaths(Collections.emptyList());
            }

            try (final var accessor = new FileChooserPathAccessor(fileSelection, fsConnection)) {
                // use getOutputPaths instead of getFSPaths,
                // that validates the wrong FilterMode.FILE from the FileChooserPathAccessor
                final var path = accessor.getOutputPath(s -> {});
                final var config = getMultiTableReadConfig().getTableReadConfig();
                final var exec = new ExecutionMonitor();
                return Map.of( //
                    path.toFSLocation().getPath(),
                    getTableReader().readSpec(path, config, exec));
            } catch (IOException | InvalidSettingsException e) {
                LOGGER.error(e);
                return computeStateFromPaths(Collections.emptyList());
            }
        }

    }

    static final class TableSpecSettingsProvider
        extends CommonReaderTransformationSettingsStateProviders.TableSpecSettingsProvider<String, DeltaTableDataType>
        implements TypedReaderTableSpecsProvider.Dependent {

        @Override
        public String toSerializableType(final DeltaTableDataType externalType) {
            return externalType.toSerializableType();
        }

        @Override
        public DeltaTableDataType toExternalType(final String serializedType) {
            return DeltaTableDataType.toExternalType(serializedType);
        }

    }

    static final class DeltaTableFSLocationsProvider extends FSLocationsProvider {

        // Modified version using a single path
        @Override
        public FSLocation[] computeState(final NodeParametersInput context)
            throws StateComputationFailureException {

            final var fileSelection = m_fileSelectionSupplier.get();
            if (!WorkflowContextUtil.hasWorkflowContext() // no workflow context available
                // no file selected (yet)
                || fileSelection.getFSLocation().equals(new FSLocation(FSCategory.LOCAL, ""))) {
                return new FSLocation[0];
            }

            var fsConnection = FileSystemPortConnectionUtil.getFileSystemConnection(context);
            if ((fileSelection.getFSLocation().getFSCategory() == FSCategory.CONNECTED) && fsConnection.isEmpty()) {
                return new FSLocation[0];
            }

            try (final var accessor = new FileChooserPathAccessor(fileSelection, fsConnection)) {
                // use getOutputPaths instead of getFSPaths,
                // that validates the wrong FilterMode.FILE from the FileChooserPathAccessor
                final var path = accessor.getOutputPath(s -> {});
                return new FSLocation[]{path.toFSLocation()};

            } catch (IOException | InvalidSettingsException e) {
                LOGGER.error(e);
                return new FSLocation[0];
            }
        }
    }

    static final class TransformationElementSettingsProvider
        extends CommonReaderTransformationSettingsStateProviders.TransformationElementSettingsProvider //
        <String, DeltaTableDataType>
        implements ProductionPathProviderAndTypeHierarchy, TypedReaderTableSpecsProvider.Dependent {

        @Override
        protected TypeReference<List<CommonReaderTransformationSettings.TableSpecSettings<String>>>
            getTableSpecSettingsTypeReference() {
            return new TypeReference<>() {
            };
        }

        @Override
        protected boolean hasMultipleFileHandling() {
            return false;
        }

        @Override
        public String toSerializableType(final DeltaTableDataType externalType) {
            return externalType.toSerializableType();
        }

        @Override
        public DeltaTableDataType toExternalType(final String serializedType) {
            return DeltaTableDataType.toExternalType(serializedType);
        }
    }

    static final class TypeChoicesProvider
        extends CommonReaderTransformationSettingsStateProviders.TypeChoicesProvider<DeltaTableDataType>
        implements ProductionPathProviderAndTypeHierarchy, TypedReaderTableSpecsProvider.Dependent {
    }

    static final class TransformationSettingsWidgetModification
        extends CommonReaderTransformationSettingsStateProviders.TransformationSettingsWidgetModification //
        <String, DeltaTableDataType> {

        @Override
        protected Class<? extends CommonReaderTransformationSettingsStateProviders.TableSpecSettingsProvider<//
                String, DeltaTableDataType>> getSpecsValueProvider() {
            return TableSpecSettingsProvider.class;
        }

        @Override
        protected
            Class<? extends CommonReaderTransformationSettingsStateProviders.TypeChoicesProvider<DeltaTableDataType>>
            getTypeChoicesProvider() {
            return TypeChoicesProvider.class;
        }

        @Override
        protected Class<TransformationElementSettingsProvider> getTransformationSettingsValueProvider() {
            return TransformationElementSettingsProvider.class;
        }

        @Override
        protected boolean hasMultipleFileHandling() {
            return false;
        }

        @Override
        @SuppressWarnings("restriction")
        public void modify(final WidgetGroupModifier group) {
            super.modify(group);
            // use modified folder source provider
            group.find(FSLocationsRef.class)//
                .modifyAnnotation(ValueProvider.class)//
                .withValue(DeltaTableFSLocationsProvider.class)//
                .modify();
        }
    }

    private DeltaTableReaderTransformationSettingsStateProviders() {
        // Not intended to be initialized
    }
}
