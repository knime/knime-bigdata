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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.swing.Box;
import javax.swing.BoxLayout;
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
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;

/**
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class DBSpark2HiveNodeDialog extends NodeDialogPane {

    private static final Border ELEMENTS_BORDER = new EmptyBorder(5, 5, 5, 5);

    private SettingsModelString m_fileFormatModel;

    private SettingsModelString m_compressionsModel;

    private SettingsModelString m_tableExistsActionModel;

    private SettingsModelString m_schemaModel;

    private SettingsModelString m_tableNameModel;

    /**
     * Creates a NodeDialog for the Spark2Hive Node
     *
     * @param fileFormats A list of supported file formats.
     *
     */
    public DBSpark2HiveNodeDialog(final FileFormat[] fileFormats) {
        final DBSpark2HiveSettings settings = new DBSpark2HiveSettings(fileFormats[0]);
        m_schemaModel = settings.getSchemaModel();
        m_tableNameModel = settings.getTableNameModel();
        m_fileFormatModel = settings.getFileFormatModel();
        m_tableExistsActionModel = settings.getTableExistsActionModel();
        m_compressionsModel = settings.getCompressionModel();

        final DialogComponentString schemaComp = new DialogComponentString(m_schemaModel, "Schema: ");

        final DialogComponentString tableNameComp = new DialogComponentString(m_tableNameModel, "Table name: ");

        final DialogComponentButtonGroup tableExistActions =
            new DialogComponentButtonGroup(m_tableExistsActionModel, null, false, TableExistsAction.values());

        final DialogComponentStringSelection compressionsComp = new DialogComponentStringSelection(m_compressionsModel,
            "Compression: ", settings.getCompressionsForFileformat(m_fileFormatModel.getStringValue()));

        m_fileFormatModel.addChangeListener(new ChangeListener() {
            /**
             * {@inheritDoc}
             */
            @Override
            public void stateChanged(final ChangeEvent e) {
                Collection<String> compressions =
                    settings.getCompressionsForFileformat(m_fileFormatModel.getStringValue());
                compressionsComp.replaceListItems(compressions, compressions.stream().findFirst().orElse("NONE"));
            }
        });
        List<String> formatsStrings = createFileFormatStringList(fileFormats);

        DialogComponentStringSelection fileFormatComp =
            new DialogComponentStringSelection(m_fileFormatModel, "File format: ", formatsStrings);

        final JPanel outerPanel = new JPanel();
        outerPanel.setLayout(new BoxLayout(outerPanel, BoxLayout.Y_AXIS));

        outerPanel.add(schemaComp.getComponentPanel());
        outerPanel.add(Box.createVerticalGlue());

        outerPanel.add(tableNameComp.getComponentPanel());
        outerPanel.add(Box.createVerticalGlue());

        outerPanel.add(tableExistActions.getComponentPanel());
        outerPanel.add(fileFormatComp.getComponentPanel());
        outerPanel.add(compressionsComp.getComponentPanel());

        outerPanel.setBorder(ELEMENTS_BORDER);

        addTab("Settings", outerPanel);
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
        m_tableNameModel.saveSettingsTo(settings);
        m_schemaModel.saveSettingsTo(settings);
        m_tableExistsActionModel.saveSettingsTo(settings);
        m_fileFormatModel.saveSettingsTo(settings);
        m_compressionsModel.saveSettingsTo(settings);

    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        try {
            m_tableNameModel.loadSettingsFrom(settings);
            m_schemaModel.loadSettingsFrom(settings);
            m_tableExistsActionModel.loadSettingsFrom(settings);
            m_fileFormatModel.loadSettingsFrom(settings);
            m_compressionsModel.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException("Unable to load settings.", e);
        }

    }

}
