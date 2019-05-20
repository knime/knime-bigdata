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
package org.knime.bigdata.spark2_4.base;

import org.knime.bigdata.spark.core.model.DefaultModelHelperProvider;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.bigdata.spark2_4.api.Spark_2_4_CompatibilityChecker;
import org.knime.bigdata.spark2_4.jobs.ml.prediction.decisiontree.classification.MLDecisionTreeClassificationModelHelper;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class Spark_2_4_MLModelHelperProvider extends DefaultModelHelperProvider<MLModel> {

    /**
     * Constructor.
     */
    public Spark_2_4_MLModelHelperProvider() {
        super(Spark_2_4_CompatibilityChecker.INSTANCE, new MLDecisionTreeClassificationModelHelper());
    }
}
