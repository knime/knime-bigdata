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
package com.knime.bigdata.spark.testing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 *
 * @author koetter
 */
public class SparkTest {

    /**
     * @param args
     */
    public static void main(final String[] args) {
        JavaSparkContext sc = null;
        try {
            //local config
            final SparkConf conf = new SparkConf().setMaster("local").setAppName("knimeTest");
            //manual execution on cluster
//            final SparkConf conf = new SparkConf().setMaster("yarn-client").setAppName("TobiasApplication");

//            conf.set("fs.default.name", "sandbox.hortonworks.com:8020");

//            System.setProperty("SPARK_CONF_DIR",
//                "C:\\DEVELOPMENT\\workspaces\\trunk\\com.knime.bigdata.spark\\conf\\spark");

//            System.setProperty("HADOOP_CONF_DIR",
//                "C:\\DEVELOPMENT\\workspaces\\trunk\\com.knime.bigdata.spark\\hadoop");
            sc = new JavaSparkContext(conf);
            List<Integer> data = Arrays.asList(1, 2, 2, 3, 4, 5);
            System.out.println(data);
            JavaRDD<Integer> distData = sc.parallelize(data);
            System.out.println(distData.collect());
            JavaRDD<Integer> distinct = distData.distinct();
            System.out.println(distinct.collect());

            int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
            int n = 10 * slices;
            List<Integer> l = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                l.add(i);
            }
            JavaRDD<Integer> dataSet = sc.parallelize(l, slices);
            int count = dataSet.map(new Function<Integer, Integer>() {
                @Override
                public Integer call(final Integer integer) {
                    double x = Math.random() * 2 - 1;
                    double y = Math.random() * 2 - 1;
                    return (x * x + y * y < 1) ? 1 : 0;
                }
            }).reduce(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(final Integer integer, final Integer integer2) {
                    return integer + integer2;
                }
            });
            System.out.println("Pi is roughly " + 4.0 * count / n);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
        System.exit(0);
    }

}
