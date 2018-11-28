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
 *   Created on 29.04.2016 by koetter
 */
package org.knime.bigdata.spark2_4.jobs.preproc.convert.number2category;

import org.knime.bigdata.spark.core.job.DefaultSimpleJobRunFactory;
import org.knime.bigdata.spark.node.preproc.convert.number2category.Number2CategoryJobInput;
import org.knime.bigdata.spark.node.preproc.convert.number2category.SparkNumber2CategoryNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Number2CategoryJobRunFactory extends DefaultSimpleJobRunFactory<Number2CategoryJobInput> {

    /**
     * Constructor.
     */
    public Number2CategoryJobRunFactory() {
        super(SparkNumber2CategoryNodeModel.JOB_ID, Number2CategoryJob.class);
    }
}
