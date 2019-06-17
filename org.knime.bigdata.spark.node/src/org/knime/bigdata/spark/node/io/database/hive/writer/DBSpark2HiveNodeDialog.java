/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 29.04.2019 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.io.database.hive.writer;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.node.io.database.hive.writer.DBSpark2HiveSettings.TableExistsAction;
import org.knime.bigdata.spark.node.io.hive.writer.FileFormat;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.database.node.component.dbrowser.DBTableSelectorDialogComponent;

/**
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class DBSpark2HiveNodeDialog extends NodeDialogPane {

    private static final Border ELEMENTS_BORDER = new EmptyBorder(5, 5, 5, 5);

    private final DBSpark2HiveSettings m_settings;
    private final DBTableSelectorDialogComponent m_table;
    private final DialogComponentButtonGroup m_tableExistsAction;
    private final DialogComponentStringSelection m_fileFormat;
    private final DialogComponentStringSelection m_compressions;

    /**
     * Creates a NodeDialog for the Spark2Hive Node
     *
     * @param fileFormats A list of supported file formats.
     *
     */
    public DBSpark2HiveNodeDialog(final FileFormat[] fileFormats) {
        m_settings = new DBSpark2HiveSettings(fileFormats[0]);
        m_table = new DBTableSelectorDialogComponent(m_settings.getSchemaAndTableModel(), 0, false, null,
            "Select a table", "Database Metadata Browser", true);
        m_tableExistsAction = new DialogComponentButtonGroup(m_settings.getTableExistsActionModel(), null, false,
            TableExistsAction.values());
        m_fileFormat = new DialogComponentStringSelection(m_settings.getFileFormatModel(), "File format: ",
            createFileFormatStringList(fileFormats));
        m_compressions = new DialogComponentStringSelection(m_settings.getCompressionModel(), "Compression: ",
            m_settings.getCompressionsForFileformat(m_settings.getFileFormat()));

        m_fileFormat.getModel().addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                final Collection<String> compressions =
                    m_settings.getCompressionsForFileformat(m_settings.getFileFormat());
                m_compressions.replaceListItems(compressions, compressions.stream().findFirst().orElse("NONE"));
            }
        });

        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(0, 5, 0, 5);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 2;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(m_table.getComponentPanel(), gbc);
        gbc.gridy++;
        gbc.weightx = 0;
        gbc.fill = GridBagConstraints.NONE;
        panel.add(createButtonGroupPanel("If table exists:", m_tableExistsAction), gbc);
        gbc.gridy++;
        panel.add(m_fileFormat.getComponentPanel(), gbc);
        gbc.gridy++;
        panel.add(m_compressions.getComponentPanel(), gbc);

        addTab("Settings", panel);
    }

    private static JPanel createButtonGroupPanel(final String label, final DialogComponentButtonGroup buttonGroup) {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(0, 5, 0, 0);
        gbc.gridx = gbc.gridy = 0;
        panel.add(new JLabel(label), gbc);
        gbc.gridx++;
        gbc.insets = new Insets(0, 0, 0, 5);
        panel.add(buttonGroup.getComponentPanel(), gbc);
        return panel;
    }

    private static List<String> createFileFormatStringList(final FileFormat[] fileFormats) {
        List<String> formatStrings = new ArrayList<>();
        for (FileFormat format : fileFormats) {
            formatStrings.add(format.toString());
        }
        return formatStrings;
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_table.saveSettingsTo(settings);
        m_tableExistsAction.saveSettingsTo(settings);
        m_fileFormat.saveSettingsTo(settings);
        m_compressions.saveSettingsTo(settings);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        m_table.loadSettingsFrom(settings, specs);
        m_tableExistsAction.loadSettingsFrom(settings, specs);
        m_fileFormat.loadSettingsFrom(settings, specs);
        m_compressions.loadSettingsFrom(settings, specs);
    }
}
