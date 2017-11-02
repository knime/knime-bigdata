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
package com.knime.bigdata.spark2_0.jobs.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.EmptyJobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.sql.SparkSQLFunctionsJobOutput;
import com.knime.bigdata.spark2_0.api.NamedObjects;
import com.knime.bigdata.spark2_0.api.SparkJob;

/**
 * Returns Spark SQL function names.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class SparkSQLFunctionsJob implements SparkJob<EmptyJobInput, SparkSQLFunctionsJobOutput> {
    private final static long serialVersionUID = 1L;

    @Override
    public SparkSQLFunctionsJobOutput runJob(final SparkContext sparkContext, final EmptyJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException, Exception {

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final List<Row> functionRows = spark.sql("SHOW ALL FUNCTIONS").collectAsList();
        final List<String> functions = new ArrayList<>(functionRows.size());
        for (Row row : functionRows) {
            functions.add(row.getString(0));
        }

        return new SparkSQLFunctionsJobOutput(functions);
    }
}
