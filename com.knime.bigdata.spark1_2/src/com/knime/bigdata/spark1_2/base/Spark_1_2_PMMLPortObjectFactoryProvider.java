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
 *   Created on 16.05.2016 by koetter
 */
package com.knime.bigdata.spark1_2.base;

import com.knime.bigdata.spark.node.pmml.converter.DefaultPMMLPortObjectFactoryProvider;
import com.knime.bigdata.spark1_2.api.Spark_1_2_CompatibilityChecker;
import com.knime.bigdata.spark1_2.jobs.mllib.clustering.kmeans.KMeansModelPMMLPortObjectFactory;
import com.knime.bigdata.spark1_2.jobs.mllib.prediction.linear.regression.LinearRegressionModelPMMLPortObjectFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark_1_2_PMMLPortObjectFactoryProvider extends DefaultPMMLPortObjectFactoryProvider {

    /**Constructor.*/
    public Spark_1_2_PMMLPortObjectFactoryProvider() {
        super(Spark_1_2_CompatibilityChecker.INSTANCE,
            new KMeansModelPMMLPortObjectFactory(),
            new LinearRegressionModelPMMLPortObjectFactory());
    }
}
