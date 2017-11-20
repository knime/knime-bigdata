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

import java.io.Serializable;
import java.util.Map;

import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput.ReplaceOperation;
import org.knime.core.data.DataColumnSpec;

/**
 * Replace missing values with exact median.
 * 
 * @author Sascha Wolke, KNIME GmbH
 */
public class MedianExactMissingValueHandler extends SparkMissingValueHandler {

    /**
     * @param col the column this handler is configured for
     */
    public MedianExactMissingValueHandler(final DataColumnSpec col) {
        super(col);
    }

    @Override
    public DerivedField getPMMLDerivedField(final Object aggResult) {
        return createValueReplacingDerivedField(getPMMLDataTypeForColumn(), aggResult.toString());
    }

    @Override
    public Map<String, Serializable> getJobInputColumnConfig(final KNIMEToIntermediateConverter converter) {
        return SparkMissingValueJobInput.createConfig(ReplaceOperation.MEDIAN_EXACT);
    }
}
