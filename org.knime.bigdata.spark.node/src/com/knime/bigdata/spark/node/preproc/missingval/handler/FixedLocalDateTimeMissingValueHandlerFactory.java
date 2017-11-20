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
package com.knime.bigdata.spark.node.preproc.missingval.handler;

import org.knime.base.node.preproc.pmml.missingval.MissingValueHandlerPanel;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.time.localdatetime.LocalDateTimeValue;

import com.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import com.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandlerFactory;

/**
 * Creates a handler that replaces missing values with a fixed date time.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class FixedLocalDateTimeMissingValueHandlerFactory extends SparkMissingValueHandlerFactory {

    /** Id of this missing value handler factory. */
    public final static String ID = "knime.FixedLocalDateTimeMissingValueHandler";

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
        return new FixedLocalDateTimeMissingValueHandlerPanel();
    }

    @Override
    public String getDisplayName() {
        return "Fix Value";
    }

    @Override
    public SparkMissingValueHandler createHandler(final DataColumnSpec column) {
        return new FixedLocalDateTimeMissingValueHandler(column);
    }

    @Override
    public boolean producesPMML4_2() {
        return true;
    }

    @Override
    public boolean isApplicable(final DataType type) {
        return type.isCompatible(LocalDateTimeValue.class);
    }
}
