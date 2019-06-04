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
 *   Created on May 27, 2019 by bjoern
 */
package org.knime.bigdata.spark.node;

import org.knime.bigdata.spark.core.model.DefaultModelHelperProvider;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.bigdata.spark.core.version.Spark2CompatibilityChecker;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification.MLDecisionTreeClassificationModelHelper;

/**
 * Model helper provider for ML models.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MLModelHelperProvider extends DefaultModelHelperProvider<MLModel> {

    /**
     * Constructor.
     */
    public MLModelHelperProvider() {
        super(Spark2CompatibilityChecker.INSTANCE, new MLDecisionTreeClassificationModelHelper());
    }
}
