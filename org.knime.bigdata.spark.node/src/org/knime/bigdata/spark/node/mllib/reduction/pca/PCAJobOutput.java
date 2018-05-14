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

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Spark PCA job output.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class PCAJobOutput extends JobOutput {

    private static final String KEY_NUMBER_OF_COMPONENTS = "numberOfComponents";

    /** Paramless constructor for automatic deserialization. */
    public PCAJobOutput(){}

    /**
     * Default constructor.
     *
     * @param numberOfComponents number of principal components
     */
    public PCAJobOutput(final int numberOfComponents) {
        set(KEY_NUMBER_OF_COMPONENTS, numberOfComponents);
    }

    /** @return number of principal components */
    public int getNumberOfComponents() {
        return getInteger(KEY_NUMBER_OF_COMPONENTS);
    }
}
