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
 */
package org.knime.bigdata.spark.node.preproc.missingval.handler;

import org.knime.base.node.preproc.pmml.missingval.MissingValueHandlerPanel;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandlerFactory;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.time.localdate.LocalDateValue;

/**
 * Creates a handler that replaces missing values with a fixed date value.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class FixedLocalDateMissingValueHandlerFactory extends SparkMissingValueHandlerFactory {

    /** Id of this missing value handler factory. */
    public final static String ID = "knime.FixedLocalDateMissingValueHandler";

    @Override
    public String getID() {
        return ID;
    }

    @Override
    public boolean hasSettingsPanel() {
        return true;
    }

    @Override
    public MissingValueHandlerPanel getSettingsPanel() {
        return new FixedLocalDateMissingValueHandlerPanel();
    }

    @Override
    public String getDisplayName() {
        return "Fix Value";
    }

    @Override
    public SparkMissingValueHandler createHandler(final DataColumnSpec column) {
        return new FixedLocalDateMissingValueHandler(column);
    }

    @Override
    public boolean producesPMML4_2() {
        return true;
    }

    @Override
    public boolean isApplicable(final DataType type) {
        return type.isCompatible(LocalDateValue.class);
    }
}
