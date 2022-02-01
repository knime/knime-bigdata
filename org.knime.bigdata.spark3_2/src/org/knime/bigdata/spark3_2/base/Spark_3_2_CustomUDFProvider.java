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
 *   Created on May 31, 2019 by bjoern
 */
package org.knime.bigdata.spark3_2.base;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class Spark_3_2_CustomUDFProvider {

    @SuppressWarnings("resource")
    public static void registerCustomUDFs(final SparkContext sparkContext) {
        final SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext).getOrCreate();

        sparkSession.udf().register("vectorToArray", new UDF1<Vector, double[]>() {
            private static final long serialVersionUID = -9059818453297516929L;

            @Override
            public double[] call(final Vector vector) throws Exception {
                return vector.toArray();
            }
        }, DataTypes.createArrayType(DataTypes.DoubleType, false));
    }

}
