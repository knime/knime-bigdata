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
 *   Created on Oct 11, 2017 by Sascha Wolke, KNIME.com
 */
package com.knime.bigdata.spark.node.preproc.missingval.handler;

import org.knime.base.node.preproc.pmml.missingval.DefaultMissingValueHandlerPanel;
import org.knime.time.util.DialogComponentDateTimeSelection;
import org.knime.time.util.DialogComponentDateTimeSelection.DisplayOption;

/**
 * A fixed missing local date panel.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class FixedLocalDateMissingValueHandlerPanel extends DefaultMissingValueHandlerPanel {
    private static final long serialVersionUID = 1L;

    /**
     * Constructor for a fixed local date panel.
     */
    public FixedLocalDateMissingValueHandlerPanel() {
        addDialogComponent(new DialogComponentDateTimeSelection(
            FixedLocalDateMissingValueHandler.createDateValueSettingsModel(), null,
            DisplayOption.SHOW_DATE_ONLY));
    }
}
