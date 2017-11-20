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
package com.knime.bigdata.spark2_2.jobs.preproc.concatenate;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.preproc.concatenate.ConcatenateRDDsJobInput;
import com.knime.bigdata.spark2_2.api.NamedObjects;
import com.knime.bigdata.spark2_2.api.SimpleSparkJob;

/**
 * Concatenates the given data frames and store result in a new data frame.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class ConcatenateDataFramesJob implements SimpleSparkJob<ConcatenateRDDsJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(ConcatenateDataFramesJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final ConcatenateRDDsJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        LOGGER.info("Concatinating data frames...");

        final List<String> inputNames = input.getNamedInputObjects();
        Dataset<Row> current = namedObjects.getDataFrame(inputNames.get(0));
        for (int i = 1; i < inputNames.size(); i++) {
            Dataset<Row> next = namedObjects.getDataFrame(inputNames.get(i));
            current = current.union(next);
        }
        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), current);

        LOGGER.info("Data frame concatenation done.");
    }
}
