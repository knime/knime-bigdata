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
package org.knime.bigdata.spark2_0.jobs.preproc.rename;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import org.knime.bigdata.spark.node.preproc.rename.RenameColumnJobInput;
import org.knime.bigdata.spark2_0.api.NamedObjects;
import org.knime.bigdata.spark2_0.api.SimpleSparkJob;

/**
 * Renames columns if column name has changed.
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
        final Dataset<Row> inputDataFrame = namedObjects.getDataFrame(namedInputObject);
        final IntermediateField newFields[] = input.getSpec(namedOutputObject).getFields();

        final String[] newNames = new String[newFields.length];
        for (int i = 0; i < newFields.length; i++) {
            newNames[i] = newFields[i].getName();
        }

        final Dataset<Row> outputDataFrame = inputDataFrame.toDF(newNames);

        namedObjects.addDataFrame(namedOutputObject, outputDataFrame);
    }
}
