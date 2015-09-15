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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.transformation;

import com.knime.bigdata.spark.node.mllib.pmml.PMMLAssignTask;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PMMLTransformationTask extends PMMLAssignTask {

    private static final long serialVersionUID = 1L;

    /**
     * Transformation task.
     */
    public PMMLTransformationTask() {
        super(true);
    }
}
