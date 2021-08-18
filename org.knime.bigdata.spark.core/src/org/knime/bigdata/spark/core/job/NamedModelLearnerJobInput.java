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
 *   Created on May 21, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.job;

import java.util.List;

/**
 * Job input superclass for all jobs that learn a "named model", i.e. a model that is put into and can be retrieved from
 * the named objects map on the Spark side.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class NamedModelLearnerJobInput extends ColumnsJobInput {

    private static final String KEY_NAMED_OUTPUT_MODEL = "namedOutputModel";

    private final static String KEY_TARGET_COLUMN_INDEX = "targetColIdx";

    private static final String KEY_HANDLE_INVALID = "handleInvalid";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public NamedModelLearnerJobInput() {
    }

    /**
     * Constructor for the supervised model learning (there is a labeled target column).
     *
     * @param namedInputObject Key/ID of the named input object (DataFrame/RDD) to learn on.
     * @param namedOutputModel Key/ID for the model that shall be produced by the job.
     * @param targetColIdx The column index of the target/class column.
     * @param featureColIdxs the feature column indices starting with 0
     */
    public NamedModelLearnerJobInput(final String namedInputObject, final String namedOutputModel,
        final int targetColIdx, final Integer[] featureColIdxs) {

        super(namedInputObject, null, featureColIdxs);
        set(KEY_TARGET_COLUMN_INDEX, targetColIdx);
        set(KEY_NAMED_OUTPUT_MODEL, namedOutputModel);
    }

    /**
     * Constructor for unsupervised model learning (no target column).
     *
     * @param namedInputObject Key/ID of the named input object (DataFrame/RDD) to learn on.
     * @param namedOutputModel Key/ID for the model that shall be produced by the job.
     * @param featureColIdxs the feature column indices starting with 0
     */
    public NamedModelLearnerJobInput(final String namedInputObject, final String namedOutputModel,
        final Integer[] featureColIdxs) {

        super(namedInputObject, null, featureColIdxs);
        set(KEY_NAMED_OUTPUT_MODEL, namedOutputModel);
    }

    public String getNamedModelId() {
        return get(KEY_NAMED_OUTPUT_MODEL);
    }

    public boolean hasTargetColumn() {
        return has(KEY_TARGET_COLUMN_INDEX);
    }

    /**
     * @return the class column index (zero-based) to train the model on.
     */
    public Integer getTargetColumnIndex() {
        return getInteger(KEY_TARGET_COLUMN_INDEX);
    }

    /**
     * @return a list with the feature column indexes to train the model on.
     */
    public List<Integer> getFeatureColumIndexes() {
        return getColumnIdxs();
    }

    /**
     * @param mode how to handle invalid values (skip, keep or error)
     */
    public void setHandleInvalid(final String mode) {
        set(KEY_HANDLE_INVALID, mode);
    }

    /**
     * @param defaultMode mode to return if option was not set
     * @return how to handle invalid values (skip, keep, error or {@code defaultMode} if not set)
     */
    public String handleInvalid(final String defaultMode) {
        if (has(KEY_HANDLE_INVALID)) {
            return get(KEY_HANDLE_INVALID);
        } else {
            return defaultMode;
        }
    }
}
