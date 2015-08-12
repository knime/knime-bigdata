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
 *   Created on 11.08.2014 by koetter
 */
package com.knime.bigdata.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

/**
 *
 * @author koetter
 */
public class SparkSQLTest {

    /**
     * @param args
     */
    public static void main(final String[] args) {
        try {
            //local config
            final SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("knimeTest");
            conf.set("hive.metastore.uris", "thrift://192.168.56.101:9083");
            final JavaSparkContext sc = new JavaSparkContext(conf);
            final JavaHiveContext sqlsc = new JavaHiveContext(sc);
            JavaSchemaRDD rdd = sqlsc.sql("select * from iris");
            final List<Row> rows = rdd.collect();
            System.out.println(rows.size() + " rows retrieved");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        System.exit(0);
    }

}
