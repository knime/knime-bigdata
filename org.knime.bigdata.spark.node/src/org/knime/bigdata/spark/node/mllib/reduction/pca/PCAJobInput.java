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
 */
package org.knime.bigdata.spark.node.mllib.reduction.pca;

import org.knime.bigdata.spark.core.job.ColumnsJobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Spark PCA job input.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class PCAJobInput extends ColumnsJobInput {

    /** Projection mode */
    public enum Mode {
        /** reduced values only */
        LEGACY,
        /** all input columns and reduced values columns appended s*/
        APPEND_COLUMNS,
        /** input columns, except columns used as input, and reduced values */
        REPLACE_COLUMNS
    }

    private static final String KEY_MODE = "mode";
    private static final String KEY_TARGET_DIMENSIONS = "targetDimensions";
    private static final String KEY_MIN_QUALITY = "minQuality";
    private static final String KEY_FAIL_ON_MISSING_VALUES = "failOnMissingValues";
    private static final String KEY_PROJ_COL_PREFIX = "projColPrefix";
    private static final String KEY_COMP_COL_PREFIX = "componentColPrefix";

    /** Paramless constructor for automatic deserialization. */
    public PCAJobInput(){}

    /**
     * Default constructor.
     *
     * @param namedInputObject input object name
     * @param pcMatrixOutputName principal components matrix output object name
     * @param projectionOutputName projection output object name
     * @param featureColIdxs feature column indices to use
     * @param failOnMV <code>true</code> if missing values should fail the job,
     *                 <code>false</code> if rows with missing values should be ignored
     * @param mode projection column mode
     * @param projColPrefix prefix of projection columns
     * @param compColPrefix prefix of principal components matrix columns
     */
    PCAJobInput(final String namedInputObject, final String pcMatrixOutputName, final String projectionOutputName,
        final Integer[] featureColIdxs, final boolean failOnMV, final Mode mode,
        final String projColPrefix, final String compColPrefix) {
        super(namedInputObject, featureColIdxs);
        addNamedOutputObject(pcMatrixOutputName);
        addNamedOutputObject(projectionOutputName);
        set(KEY_FAIL_ON_MISSING_VALUES, failOnMV);
        set(KEY_MODE, mode);
        set(KEY_PROJ_COL_PREFIX, projColPrefix);
        set(KEY_COMP_COL_PREFIX, compColPrefix);
    }

    /** @return principal component matrix name */
    public String getPCMatrixOutputName() {
        return getNamedOutputObjects().get(0);
    }

    /** @return output projection object name */
    public String getProjectionOutputName() {
        return getNamedOutputObjects().get(1);
    }

    /** @return <code>true</code> if number of principal components should be computed */
    public boolean computeTargetDimension() {
        return has(KEY_MIN_QUALITY);
    }

    /** @param dimensions target number of principal components */
    public void setTargetDimensions(final int dimensions) {
        set(KEY_TARGET_DIMENSIONS, dimensions);
    }

    /** @return number of principal components */
    public int getTargetDimensions() {
        return getInteger(KEY_TARGET_DIMENSIONS);
    }

    /** @param minQuality minimum information fraction to preserve */
    public void setMinQuality(final double minQuality) {
        set(KEY_MIN_QUALITY, minQuality);
    }

    /** @return minimum quality / information fraction to preserve */
    public double getMinQuality() {
        return getDouble(KEY_MIN_QUALITY);
    }

    /** @return <code>true</code> if missing values should fail the job,
     *          <code>false</code> if rows with missing values should be ignored */
    public boolean failOnMissingValues() {
        return get(KEY_FAIL_ON_MISSING_VALUES);
    }

    /** @return Projections output columns mode */
    public Mode getMode() {
        return get(KEY_MODE);
    }

    /** @return prefix of projection columns */
    public String getProjColPrefix() {
        return get(KEY_PROJ_COL_PREFIX);
    }

    /** @return prefix of principal components matrix columns */
    public String getCompColPrefix() {
        return get(KEY_COMP_COL_PREFIX);
    }
}
