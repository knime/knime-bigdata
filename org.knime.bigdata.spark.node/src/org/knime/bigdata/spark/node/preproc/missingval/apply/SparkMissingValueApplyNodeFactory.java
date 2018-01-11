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
package org.knime.bigdata.spark.node.preproc.missingval.apply;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 * Missing value apply Spark node factory.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkMissingValueApplyNodeFactory extends DefaultSparkNodeFactory<SparkMissingValueApplyNodeModel> {

    /** Missing value spark node factory */
    public SparkMissingValueApplyNodeFactory() {
        super("column/transform");
    }

    @Override
    public SparkMissingValueApplyNodeModel createNodeModel() {
        return new SparkMissingValueApplyNodeModel();
    }
}
