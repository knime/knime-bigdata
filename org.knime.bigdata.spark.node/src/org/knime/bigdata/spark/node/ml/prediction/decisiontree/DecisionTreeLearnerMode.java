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
 *   Created on May 20, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

/**
 * Model learner nodes that have been ported from spark.mllib (deprecated) to spark.ml share code (settings, node
 * dialog) between the mllib (and ml node code. This enum is used in various places where the mllib-specific code
 * differs from the ml-specific code.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public enum DecisionTreeLearnerMode {

    /**
     * Indicates deprecated mllib-specific behavior.
     */
    DEPRECATED,

    /**
     * Indicates ml-specific behavior for a classification model learner.
     */
    CLASSIFICATION,

    /**
     * Indicates ml-specific behavior for a regression model learner.
     */
    REGRESSION;
}
