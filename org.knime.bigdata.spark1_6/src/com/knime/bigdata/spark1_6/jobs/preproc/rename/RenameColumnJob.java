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
 *   Created on Jan 17, 2017 by Sascha Wolke, KNIME.com
 */
package com.knime.bigdata.spark1_6.jobs.preproc.rename;

import org.apache.spark.SparkContext;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.preproc.rename.RenameColumnJobInput;
import com.knime.bigdata.spark1_6.api.NamedObjects;
import com.knime.bigdata.spark1_6.api.SimpleSparkJob;

/**
 * Dummy job that stores input RDD as output RDD without any modification.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class RenameColumnJob implements SimpleSparkJob<RenameColumnJobInput> {
    private final static long serialVersionUID = 1L;

    @Override
    public void runJob(final SparkContext sparkContext, final RenameColumnJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addJavaRdd(namedOutputObject, namedObjects.getJavaRdd(namedInputObject));
    }
}
