/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on 21.08.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.convert.number2category;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkNumber2CategroyNodeDialog extends DefaultNodeSettingsPane {

    SparkNumber2CategroyNodeDialog() {
        final SettingsModelBoolean keepModel = SparkNumber2CategoryNodeModel.createKeepOriginalColsModel();
        final SettingsModelString suffixModel = SparkNumber2CategoryNodeModel.createColSuffixModel();
        keepModel.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                suffixModel.setEnabled(keepModel.getBooleanValue());
            }
        });
        addDialogComponent(new DialogComponentBoolean(keepModel, "Keep original columns"));
        addDialogComponent(new DialogComponentString(suffixModel, "Column suffix: ", true, 30));
    }
}
