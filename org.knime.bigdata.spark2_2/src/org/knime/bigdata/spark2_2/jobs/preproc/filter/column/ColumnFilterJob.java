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
package org.knime.bigdata.spark2_2.jobs.preproc.filter.column;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.ColumnsJobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark2_2.api.NamedObjects;
import org.knime.bigdata.spark2_2.api.SimpleSparkJob;

/**
 * Select given columns from input table and store result in new data frame.
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class ColumnFilterJob implements SimpleSparkJob<ColumnsJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(ColumnFilterJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final ColumnsJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException {

        LOGGER.info("Starting Column Selection job...");
        final Dataset<Row> inputDataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final String allColumns[] = inputDataset.columns();
        final String selectedColumnNames[] = input.getColumnNames(allColumns);
        final Column selectedColumns[] = new Column[selectedColumnNames.length];

        for (int i = 0; i < selectedColumnNames.length; i++) {
            selectedColumns[i] = inputDataset.col(selectedColumnNames[i]);
        }

        final Dataset<Row> result = inputDataset.select(selectedColumns);
        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), result);

        LOGGER.info("Column Selection done");
    }
}
