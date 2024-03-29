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
package org.knime.bigdata.spark1_6.jobs.hive;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructField;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.io.hive.reader.Hive2SparkJobOutput;
import org.knime.bigdata.spark.node.io.hive.reader.Hive2SparkJobInput;
import org.knime.bigdata.spark1_6.api.NamedObjects;
import org.knime.bigdata.spark1_6.api.SparkJob;
import org.knime.bigdata.spark1_6.api.TypeConverters;
import org.knime.bigdata.spark1_6.hive.HiveContextProvider;
import org.knime.bigdata.spark1_6.hive.HiveContextProvider.HiveContextAction;

/**
 * executes given sql statement and puts result into a (named) JavaRDD
 *
 * @author dwk, jfr
 */
@SparkClass
public class Hive2SparkJob implements SparkJob<Hive2SparkJobInput, Hive2SparkJobOutput> {
    private static final long serialVersionUID = 1L;

    private static final  Logger LOGGER = Logger.getLogger(Hive2SparkJob.class.getName());

    @Override
    public Hive2SparkJobOutput runJob(final SparkContext sparkContext, final Hive2SparkJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException {

        LOGGER.log(Level.INFO, "reading hive table...");

        LOGGER.log(Level.INFO, "sql statement: " + input.getQuery());

        final DataFrame dataFrame = HiveContextProvider.runWithHiveContext(sparkContext, new HiveContextAction<DataFrame>() {
            @Override
            public DataFrame runWithHiveContext(final HiveContext hiveContext) {
                return hiveContext.sql(input.getQuery());
            }
        });

        for (final StructField field : dataFrame.schema().fields()) {
            LOGGER.log(Level.FINE, "Field '" + field.name() + "' of type '" + field.dataType() + "'");
        }

        final RDD<Row> rdd = dataFrame.rdd();
        final JavaRDD<Row> javaRDD = new JavaRDD<>(rdd, rdd.elementClassTag());

        final String key = input.getFirstNamedOutputObject();
        LOGGER.log(Level.INFO, "Storing Hive query result under key: {0}" , key);
        namedObjects.addJavaRdd(key, javaRDD);
        LOGGER.log(Level.INFO, "done");
        return new Hive2SparkJobOutput(key, TypeConverters.convertSpec(dataFrame.schema()));
    }
}
