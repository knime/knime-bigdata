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
 *   Created on 12.02.2015 by koetter
 */
package org.knime.bigdata.spark.node.pmml.predictor.compiling;

import org.knime.bigdata.spark.node.pmml.predictor.AbstractSparkPMMLPredictorNodeFactory;

/**
 *
 * @author koetter
 */
public class SparkPMMLCompilingPredictorNodeFactory
    extends AbstractSparkPMMLPredictorNodeFactory<SparkPMMLCompilingPredictorNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkPMMLCompilingPredictorNodeModel createNodeModel() {
        return new SparkPMMLCompilingPredictorNodeModel();
    }
}
