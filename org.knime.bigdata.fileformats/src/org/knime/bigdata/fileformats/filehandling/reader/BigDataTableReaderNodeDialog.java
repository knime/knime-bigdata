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
 *   Sep 30, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader;

import java.awt.GridBagLayout;
import java.util.Arrays;
import java.util.stream.Stream;

import javax.swing.BorderFactory;
import javax.swing.JCheckBox;
import javax.swing.JPanel;

import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.core.data.convert.map.ProductionPath;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.data.location.variable.FSLocationVariableType;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.DialogComponentReaderFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.SettingsModelReaderFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;
import org.knime.filehandling.core.node.table.reader.MultiTableReadFactory;
import org.knime.filehandling.core.node.table.reader.ProductionPathProvider;
import org.knime.filehandling.core.node.table.reader.config.StorableMultiTableReadConfig;
import org.knime.filehandling.core.node.table.reader.dialog.SourceIdentifierColumnPanel;
import org.knime.filehandling.core.node.table.reader.preview.dialog.AbstractTableReaderNodeDialog;
import org.knime.filehandling.core.node.table.reader.preview.dialog.GenericItemAccessor;
import org.knime.filehandling.core.util.GBCBuilder;
import org.knime.filehandling.core.util.SettingsUtils;

/**
 * Dialog for the Parquet and ORC Reader nodes.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class BigDataTableReaderNodeDialog
    extends AbstractTableReaderNodeDialog<FSPath, BigDataReaderConfig, KnimeType> {

    private final BigDataMultiTableReadConfig m_config;

    private static final String TRANSFORMATION_TAB = "Transformation";

    private final DialogComponentReaderFileChooser m_fileChooser;

    private final JCheckBox m_failOnDifferingSpecs = new JCheckBox("Fail on differing specs");

    private final JCheckBox m_supportChangingFileSchemas = new JCheckBox("Support changing file schemas");

    private final SourceIdentifierColumnPanel m_pathColumnPanel = new SourceIdentifierColumnPanel("Path");

    /**
     * Constructor.
     *
     * @param pathSettings the {@link SettingsModelReaderFileChooser} for selecting files
     * @param config {@link StorableMultiTableReadConfig} for managing the node configuration
     * @param readFactory {@link MultiTableReadFactory} for the actual reading
     * @param productionPathProvider {@link ProductionPathProvider} providing {@link ProductionPath ProductionPaths}
     */
    public BigDataTableReaderNodeDialog(final SettingsModelReaderFileChooser pathSettings,
        final BigDataMultiTableReadConfig config,
        final MultiTableReadFactory<FSPath, BigDataReaderConfig, KnimeType> readFactory,
        final ProductionPathProvider<KnimeType> productionPathProvider) {
        super(readFactory, productionPathProvider, true);
        m_config = config;
        final String[] keyChain =
            Stream.concat(Stream.of(SettingsUtils.CFG_SETTINGS_TAB), Arrays.stream(pathSettings.getKeysForFSLocation()))
                .toArray(String[]::new);
        final FlowVariableModel locationFvm = createFlowVariableModel(keyChain, FSLocationVariableType.INSTANCE);
        m_fileChooser = new DialogComponentReaderFileChooser(pathSettings, "parquet", locationFvm);
        pathSettings.addChangeListener(e -> handlePathSettingsChange());
        m_failOnDifferingSpecs.addActionListener(e -> configChanged());
        m_supportChangingFileSchemas.addActionListener(e -> configChanged());
        m_supportChangingFileSchemas.addActionListener(e -> updateTransformationTableEnabledStatus());
        m_pathColumnPanel.addChangeListener(e -> configChanged());
        addTab("Settings", createSettingsPanel());
        addTab(TRANSFORMATION_TAB, createTransformationTab());
    }

    private void updateTransformationTableEnabledStatus() {
        setEnabled(!m_supportChangingFileSchemas.isSelected(), TRANSFORMATION_TAB);
    }

    private void handlePathSettingsChange() {
        updateMultiFileEnabledStatus();
        configChanged();
    }

    private void updateMultiFileEnabledStatus() {
        final boolean isMultiFile = isMultiFile();
        m_failOnDifferingSpecs.setEnabled(isMultiFile);
        getTransformationPanel().setColumnFilterModeEnabled(isMultiFile);
    }

    private boolean isMultiFile() {
        return m_fileChooser.getSettingsModel().getFilterMode() != FilterMode.FILE;
    }

    private JPanel createSettingsPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GBCBuilder gbc = new GBCBuilder().resetPos().anchorFirstLineStart().fillHorizontal().setWeightX(1.0);
        final JPanel fileChooserPanel = m_fileChooser.getComponentPanel();
        fileChooserPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Input location"));
        panel.add(fileChooserPanel, gbc.build());
        panel.add(createMultiFilePanel(), gbc.incY().build());
        panel.add(createTableSpecificationPanel(), gbc.incY().build());
        panel.add(m_pathColumnPanel, gbc.incY().build());
        panel.add(createPreview(), gbc.incY().fillBoth().setWeightY(1.0).build());
        return panel;
    }

    private JPanel createMultiFilePanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(
            BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Options for multiple files"));
        GBCBuilder gbc = new GBCBuilder().resetPos().anchorFirstLineStart().fillHorizontal();
        panel.add(m_failOnDifferingSpecs, gbc.build());
        panel.add(new JPanel(), gbc.incX().setWeightX(1.0).build());
        return panel;
    }

    private JPanel createTableSpecificationPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Table specification"));
        final GBCBuilder gbc = new GBCBuilder().resetPos().anchorFirstLineStart().fillHorizontal();
        panel.add(m_supportChangingFileSchemas, gbc.build());
        panel.add(new JPanel(), gbc.incX().setWeightX(1.0).build());
        return panel;
    }

    @Override
    protected BigDataMultiTableReadConfig getConfig() throws InvalidSettingsException {
        saveToConfig();
        return m_config;
    }

    @SuppressWarnings("resource") // the ReadPathAccessor is managed by the adapter
    @Override
    protected GenericItemAccessor<FSPath> createItemAccessor() {
        return new ReadPathAccessorAdapter(m_fileChooser.getSettingsModel().createReadPathAccessor());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        super.saveSettingsTo(settings);
        m_fileChooser.saveSettingsTo(SettingsUtils.getOrAdd(settings, SettingsUtils.CFG_SETTINGS_TAB));
        saveToConfig();
        m_config.saveInDialog(settings);
    }

    private void saveToConfig() {
        m_config.setFailOnDifferingSpecs(m_failOnDifferingSpecs.isSelected());
        m_config.setAppendItemIdentifierColumn(m_pathColumnPanel.isAppendSourceIdentifierColumn());
        m_config.setItemIdentifierColumnName(m_pathColumnPanel.getSourceIdentifierColumnName());
        final boolean saveTableSpecConfig = !m_supportChangingFileSchemas.isSelected();
        m_config.setSaveTableSpecConfig(saveTableSpecConfig);
        m_config.setTableSpecConfig(saveTableSpecConfig ? getTableSpecConfig() : null);
    }

    @Override
    protected BigDataMultiTableReadConfig loadSettings(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        m_fileChooser.loadSettingsFrom(SettingsUtils.getOrEmpty(settings, SettingsUtils.CFG_SETTINGS_TAB), specs);
        m_config.loadInDialog(settings, specs);
        m_failOnDifferingSpecs.setSelected(m_config.failOnDifferingSpecs());
        m_pathColumnPanel.load(m_config.appendItemIdentifierColumn(), m_config.getItemIdentifierColumnName());
        updateMultiFileEnabledStatus();
        m_supportChangingFileSchemas.setSelected(!m_config.saveTableSpecConfig());
        updateTransformationTableEnabledStatus();
        return m_config;
    }

    @Override
    public void onClose() {
        m_fileChooser.onClose();
    }

}
