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
 *   Created on Apr 13, 2016 by bjoern
 */
package org.knime.bigdata.spark3_1.jobs.mllib.clustering.kmeans;

import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.node.mllib.clustering.kmeans.MLlibKMeansNodeModel;
import org.knime.bigdata.spark3_1.api.Spark_3_1_MLlibModelHelper;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class KMeansModelHelper extends Spark_3_1_MLlibModelHelper {

    /**Constructor.*/
    public KMeansModelHelper() {
        super(MLlibKMeansNodeModel.MODEL_NAME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelInterpreter getModelInterpreter() {
        return KMeansInterpreter.getInstance();
    }
}
