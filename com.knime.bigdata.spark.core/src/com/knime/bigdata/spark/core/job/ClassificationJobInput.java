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
 *   Created on 06.05.2016 by koetter
 */
package com.knime.bigdata.spark.core.job;

import java.util.List;

/**
 * {@link JobInput} implementation of a classification job with a class column index as well as
 * feature column indices.
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class ClassificationJobInput extends ColumnsJobInput {
    private final static String KEY_CLASS_COLUMN_INDEX = "classColIdxs";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public ClassificationJobInput() {}

    /**
     * @param namedInputObject the input data to classify
     * @param classColIdx the class column index starting with 0
     * @param featureColIdxs the feature column indices starting with 0
     *
     */
    public ClassificationJobInput(final String namedInputObject, final Integer classColIdx, final List<Integer> featureColIdxs) {
        this(namedInputObject, classColIdx, featureColIdxs.toArray(new Integer[0]));
    }

    /**
     * @param namedInputObject the input data to classify
     * @param classColIdx the class column index starting with 0
     * @param featureColIdxs the feature column indices starting with 0
     *
     */
    public ClassificationJobInput(final String namedInputObject, final Integer classColIdx, final Integer... featureColIdxs) {
        super(namedInputObject, featureColIdxs);
        set(KEY_CLASS_COLUMN_INDEX, classColIdx);
    }


    /**
     * @return the class column index starting with 0
     */
    public Integer getClassColIdx() {
        return getInteger(KEY_CLASS_COLUMN_INDEX);
    }
}
