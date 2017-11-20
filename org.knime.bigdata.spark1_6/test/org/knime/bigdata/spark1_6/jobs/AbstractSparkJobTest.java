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
 *   Created on Feb 6, 2017 by Sascha Wolke, KNIME.com
 */
package org.knime.bigdata.spark1_6.jobs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.knime.bigdata.spark1_6.api.RowBuilder;

/**
 * Spark tests basis.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SuppressWarnings({"javadoc", "serial"})
public abstract class AbstractSparkJobTest implements Serializable {

    protected transient static JavaSparkContext javaSparkContext;

    protected SimpleNamedObjectHandler namedObjecs = new SimpleNamedObjectHandler();

    @BeforeClass
    public static void setUpSpark() throws Exception {
        final SparkConf config = new SparkConf()
                .setMaster("local[1]")
                .setAppName("tests")
                .set("spark.ui.enabled", "false");
        javaSparkContext = new JavaSparkContext(config);
        Logger.getRootLogger().setLevel(Level.WARN);
        Logger.getLogger("com.knime").setLevel(Level.DEBUG);
    }

    @Before
    public void resetNamedObjects() {
        namedObjecs.clear();
    }

    @AfterClass
    public static void shutdownSpark() throws Exception {
        if (javaSparkContext != null) {
            javaSparkContext.stop();
            javaSparkContext = null;
        }
    }

    protected List<Integer> asList(final int... input) {
        final ArrayList<Integer> result = new ArrayList<>(input.length);
        for (Integer i : input) {
            result.add(i);
        }
        return result;
    }

    protected JavaRDD<Row> sortByInteger(final JavaRDD<Row> rows, final int numPartitions) {
        return rows.sortBy(new Function<Row, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(final Row r) throws Exception {
                return r.getInt(0);
            }
        }, true, numPartitions);
    }

    protected JavaRDD<Row> asRow(final int... input) {
        return asRow(asList(input));
    }

    protected <T> JavaRDD<Row> asRow(final List<T> input) {
        return javaSparkContext.parallelize(input).map(new Function<T, Row>() {
            @Override
            public Row call(final T i) throws Exception {
                return RowBuilder.emptyRow().add(i).build();
            }
        });
    }

    protected Integer[] asIntArray(final JavaRDD<Row> input) {
        return asIntArray(input, false);
    }

    protected Integer[] asSortedIntArray(final JavaRDD<Row> input) {
        return asIntArray(input, true);
    }

    protected Integer[] asIntArray(final JavaRDD<Row> input, final boolean sort) {
        JavaRDD<Integer> result = input.map(new Function<Row, Integer>() {
            @Override
            public Integer call(final Row row) throws Exception {
                return row.getInt(0);
            }
        });

        if (sort) {
            result = result.sortBy(new Function<Integer, Integer>() {
                @Override
                public Integer call(final Integer i) throws Exception {
                    return i;
                }
            }, true, 1);
        }

        return result.collect().toArray(new Integer[0]);
    }

    protected String[] asStringArray(final JavaRDD<Row> input, final boolean sort) {
        JavaRDD<String> result = input.map(new Function<Row, String>() {
            @Override
            public String call(final Row row) throws Exception {
                return row.getString(0);
            }
        });

        if (sort) {
            result = result.sortBy(new Function<String, String>() {
                @Override
                public String call(final String s) throws Exception {
                    return s == null ? "" : s;
                }
            }, true, 1);
        }

        return result.collect().toArray(new String[0]);
    }

    protected int[] intRange(final int start, final int end) {
        int[] result = new int[end-start+1];
        for (int i = start; i <= end; i++) {
            result[i - start] = i;
        }
        return result;
    }

    protected Integer[] integerRange(final int start, final int end) {
        Integer[] result = new Integer[end-start+1];
        for (int i = start; i <= end; i++) {
            result[i - start] = i;
        }
        return result;
    }
}
