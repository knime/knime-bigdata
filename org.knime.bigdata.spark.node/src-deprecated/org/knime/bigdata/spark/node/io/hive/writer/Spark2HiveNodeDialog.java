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
 *   Created on 07.06.2015 by koetter
 */
package org.knime.bigdata.spark.node.io.hive.writer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 *
 * @author koetter
 */
@Deprecated
public class Spark2HiveNodeDialog extends DefaultNodeSettingsPane {

    /**
     * Creates a NodeDialog for the Spark2Hive Node
     *
     * @param fileFormats A list of supported file formats.
     *
     */
    public Spark2HiveNodeDialog(final FileFormat[] fileFormats) {
        final Spark2HiveSettings settings = new Spark2HiveSettings(fileFormats[0]);
        addDialogComponent(new DialogComponentString(settings.getTableNameModel(), "Table name: ", true, 20));
        addDialogComponent(new DialogComponentBoolean(settings.getDropExistingModel(), "Drop existing table"));

        final SettingsModelString fileFormatModel = settings.getFileFormatModel();
        final DialogComponentStringSelection compressionsComp =
            new DialogComponentStringSelection(settings.getCompressionModel(), "Compression: ",
                settings.getCompressionsForFileformat(fileFormatModel.getStringValue()));

        fileFormatModel.addChangeListener(new ChangeListener() {
            /**
             * {@inheritDoc}
             */
            @Override
            public void stateChanged(final ChangeEvent e) {
                Collection<String> compressions =
                    settings.getCompressionsForFileformat(fileFormatModel.getStringValue());
                compressionsComp.replaceListItems(compressions, compressions.stream().findFirst().orElse("NONE"));
            }
        });
        List<String> formatsStrings = createStringList(fileFormats);

        DialogComponentStringSelection fileFormatComp =
            new DialogComponentStringSelection(fileFormatModel, "File format: ", formatsStrings);
        addDialogComponent(fileFormatComp);

        addDialogComponent(compressionsComp);
    }

    private List<String> createStringList(final FileFormat[] fileFormats) {
        List<String> formatStrings = new ArrayList<>();
        for (FileFormat format : fileFormats) {
            formatStrings.add(format.toString());
        }
        return formatStrings;
    }
}
