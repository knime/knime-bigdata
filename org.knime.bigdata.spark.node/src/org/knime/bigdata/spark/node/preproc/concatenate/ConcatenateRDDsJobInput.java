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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark.node.preproc.concatenate;

import java.util.Arrays;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author dwk
 */
@SparkClass
public class ConcatenateRDDsJobInput extends JobInput {

    /**
     * Paramless constructor for automatic deserialization.
     */
    public ConcatenateRDDsJobInput() {}

    ConcatenateRDDsJobInput(final String[] aInputRDDs, final String aOutputTable) {
        addNamedInputObjects(Arrays.asList(aInputRDDs));
        addNamedOutputObject(aOutputTable);
    }
}
