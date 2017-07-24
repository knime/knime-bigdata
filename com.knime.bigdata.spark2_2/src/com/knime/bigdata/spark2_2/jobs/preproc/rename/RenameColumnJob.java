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
 *   Created on Jan 17, 2017 by Sascha Wolke, KNIME.com
 */
package com.knime.bigdata.spark2_2.jobs.preproc.rename;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.preproc.rename.RenameColumnJobInput;
import com.knime.bigdata.spark2_2.api.NamedObjects;
import com.knime.bigdata.spark2_2.api.SimpleSparkJob;
import com.knime.bigdata.spark2_2.api.TypeConverters;

/**
 * Renames columns via schema update.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class RenameColumnJob implements SimpleSparkJob<RenameColumnJobInput> {
    private final static long serialVersionUID = 1L;

    @Override
    public void runJob(final SparkContext sparkContext, final RenameColumnJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException, Exception {

        final SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final String namedInputObject = input.getFirstNamedInputObject();
        final String namedOutputObject = input.getFirstNamedOutputObject();
        final Dataset<Row> inputDataFrame = namedObjects.getDataFrame(namedInputObject);
        final StructType outputSchema = TypeConverters.convertSpec(input.getSpec(namedOutputObject));
        final Dataset<Row> outputDataFrame = sparkSession.createDataFrame(inputDataFrame.javaRDD(), outputSchema);
        namedObjects.addDataFrame(namedOutputObject, outputDataFrame);
    }
}
