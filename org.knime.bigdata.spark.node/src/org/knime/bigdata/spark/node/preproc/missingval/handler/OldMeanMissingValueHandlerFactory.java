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

import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandlerFactory;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;

/**
 * Old mean missing values handler factory (introduced in BD-224), that truncates the mean on integer and long columns
 * (output data types are not changed).
 *
 * @author Sascha Wolke, KNIME GmbH
 * @deprecated use {@link DoubleMeanMissingValueHandlerFactory} or {@link RoundedMeanMissingValueHandlerFactory} instead
 */
@Deprecated
public class OldMeanMissingValueHandlerFactory extends SparkMissingValueHandlerFactory {

    /** Id of this missing value handler factory. */
    public static final String ID = "knime.MeanMissingValueHandler";

    @Override
    public String getID() {
        return ID;
    }

    @Override
    public boolean isDeprecated() {
        return true;
    }

    @Override
    public String getDisplayName() {
        return "Mean (deprecated)";
    }

    @Override
    public SparkMissingValueHandler createHandler(final DataColumnSpec column) {
        return new OldMeanMissingValueHandler(column);
    }

    @Override
    public boolean producesPMML4_2() {
        return true;
    }

    @Override
    public boolean isApplicable(final DataType type) {
        return type.equals(DoubleCell.TYPE) || type.equals(IntCell.TYPE) || type.equals(LongCell.TYPE);
    }
}
