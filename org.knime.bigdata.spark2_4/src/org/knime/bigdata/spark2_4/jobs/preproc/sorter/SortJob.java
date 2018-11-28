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
 */
package org.knime.bigdata.spark2_4.jobs.preproc.sorter;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.preproc.sorter.SortJobInput;
import org.knime.bigdata.spark2_4.api.NamedObjects;
import org.knime.bigdata.spark2_4.api.SimpleSparkJob;

/**
 * Sorts input data frame by given indices, in given order.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class SortJob implements SimpleSparkJob<SortJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(SortJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final SortJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        LOGGER.info("Starting sort job...");
        final Dataset<Row> inputDataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final Integer[] colIndex = input.getFeatureColIdxs();
        final String[] colNames = inputDataset.columns();
        final Boolean[] sortOrders = input.isSortDirectionAscending();
        final Boolean missingToEnd = input.missingToEnd();
        final Column[] sortColumns = new Column[colIndex.length];

        for (int i = 0; i < colIndex.length; i++) {
            final String name = colNames[colIndex[i]];
            if (sortOrders[i] && missingToEnd) {
                sortColumns[i] = inputDataset.col(name).asc_nulls_last();
            } else if (sortOrders[i]) {
                sortColumns[i] = inputDataset.col(name).asc();
            } else if (missingToEnd) {
                sortColumns[i] = inputDataset.col(name).desc_nulls_last();
            } else {
                sortColumns[i] = inputDataset.col(name).desc();
            }
        }

        final Dataset<Row> resultDataset = inputDataset.sort(sortColumns);
        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), resultDataset);

        LOGGER.info("Sort done");
    }
}
