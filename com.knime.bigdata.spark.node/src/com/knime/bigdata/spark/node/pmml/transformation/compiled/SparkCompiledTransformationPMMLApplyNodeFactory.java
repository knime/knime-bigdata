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
 *   Created on 03.08.2015 by dwk
 */
package com.knime.bigdata.spark.node.pmml.transformation.compiled;

import com.knime.bigdata.spark.node.pmml.transformation.AbstractSparkTransformationPMMLApplyNodeFactory;

/**
 *
 * @author dwk
 */
public class SparkCompiledTransformationPMMLApplyNodeFactory
extends AbstractSparkTransformationPMMLApplyNodeFactory<SparkCompiledTransformationPMMLApplyNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkCompiledTransformationPMMLApplyNodeModel createNodeModel() {
        return new SparkCompiledTransformationPMMLApplyNodeModel();
    }
}
