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
 *   Nov 6, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader;

import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeTypeHierarchies;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.SettingsModelReaderFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;
import org.knime.filehandling.core.node.table.reader.AbstractTableReaderNodeFactory;
import org.knime.filehandling.core.node.table.reader.MultiTableReadFactory;
import org.knime.filehandling.core.node.table.reader.ProductionPathProvider;
import org.knime.filehandling.core.node.table.reader.ReadAdapterFactory;
import org.knime.filehandling.core.node.table.reader.config.DefaultMultiTableReadConfig;
import org.knime.filehandling.core.node.table.reader.config.DefaultTableReadConfig;
import org.knime.filehandling.core.node.table.reader.preview.dialog.AbstractTableReaderNodeDialog;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TypeHierarchy;

/**
 * {@link AbstractTableReaderNodeFactory} for readers of big data file formats like ORC and Parquet.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractBigDataTableReaderNodeFactory
    extends AbstractTableReaderNodeFactory<BigDataReaderConfig, KnimeType, BigDataCell> {

    private final String[] m_fileExtensions;

    /**
     * Constructor.
     *
     * @param fileExtensions the supported file extensions
     */
    protected AbstractBigDataTableReaderNodeFactory(final String[] fileExtensions) {
        m_fileExtensions = fileExtensions;
    }

    @Override
    protected final SettingsModelReaderFileChooser
        createPathSettings(final NodeCreationConfiguration nodeCreationConfig) {
        return new SettingsModelReaderFileChooser("file_selection", nodeCreationConfig.getPortConfig().get(),
            FS_CONNECT_GRP_ID, FilterMode.FILE, m_fileExtensions);
    }

    @Override
    protected final AbstractTableReaderNodeDialog<BigDataReaderConfig, KnimeType> createNodeDialogPane(
        final NodeCreationConfiguration creationConfig,
        final MultiTableReadFactory<BigDataReaderConfig, KnimeType> readFactory,
        final ProductionPathProvider<KnimeType> defaultProductionPathFn) {
        return new BigDataTableReaderNodeDialog<>(createPathSettings(creationConfig), createConfig(), readFactory,
            defaultProductionPathFn);
    }

    @Override
    protected final DefaultMultiTableReadConfig<BigDataReaderConfig, DefaultTableReadConfig<BigDataReaderConfig>>
        createConfig() {
        final DefaultTableReadConfig<BigDataReaderConfig> tc = new DefaultTableReadConfig<>(new BigDataReaderConfig());
        return new DefaultMultiTableReadConfig<>(tc, BigDataTableReadConfigSerializer.INSTANCE);
    }

    @Override
    protected final ReadAdapterFactory<KnimeType, BigDataCell> getReadAdapterFactory() {
        return BigDataReadAdapterFactory.INSTANCE;
    }

    @Override
    protected final String extractRowKey(final BigDataCell value) {
        return value.getString();
    }

    @Override
    protected final TypeHierarchy<KnimeType, KnimeType> getTypeHierarchy() {
        return KnimeTypeHierarchies.TYPE_HIERARCHY;
    }

}
