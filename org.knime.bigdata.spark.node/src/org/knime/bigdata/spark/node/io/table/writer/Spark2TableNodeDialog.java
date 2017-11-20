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
 *   Created on 26.06.2015 by koetter
 */
package org.knime.bigdata.spark.node.io.table.writer;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark2TableNodeDialog extends DefaultNodeSettingsPane {

    /**
     *
     */
    Spark2TableNodeDialog() {
        final SettingsModelIntegerBounded fetchSizeModel = Spark2TableNodeModel.createFetchSizeModel();
        final SettingsModelBoolean fetchAllModel = Spark2TableNodeModel.createFetchAllModel();
        fetchAllModel.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                fetchSizeModel.setEnabled(!fetchAllModel.getBooleanValue());
            }
        });
        addDialogComponent(new DialogComponentBoolean(fetchAllModel, "Fetch all rows"));
        addDialogComponent(new DialogComponentNumber(fetchSizeModel, "No of rows to fetch", 1000));
    }
}
