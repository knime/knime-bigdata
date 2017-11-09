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

import java.io.Serializable;
import java.util.Map;

import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.knime.core.data.DataColumnSpec;

import com.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import com.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import com.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput;
import com.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput.ReplaceOperation;

/**
 * Replaces missing values in a column with the smallest value in this column.
 * 
 * @author Sascha Wolke, KNIME GmbH
 */
public class MinMissingValueHandler extends SparkMissingValueHandler {

    /**
     * @param col the column this handler is configured for
     */
    public MinMissingValueHandler(final DataColumnSpec col) {
        super(col);
    }

    @Override
    public DerivedField getPMMLDerivedField(final Object aggResult) {
        return createValueReplacingDerivedField(getPMMLDataTypeForColumn(), aggResult.toString());
    }

    @Override
    public Map<String, Serializable> getJobInputColumnConfig(final KNIMEToIntermediateConverter converter) {
        return SparkMissingValueJobInput.createConfig(ReplaceOperation.MIN);
    }
}
