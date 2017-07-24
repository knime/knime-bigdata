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
 */
package com.knime.bigdata.spark2_1.base;

import com.knime.bigdata.spark2_1.jobs.scripting.java.AbstractSparkJavaSnippet;
import com.knime.bigdata.spark2_1.jobs.scripting.java.AbstractSparkJavaSnippetSink;
import com.knime.bigdata.spark2_1.jobs.scripting.java.AbstractSparkJavaSnippetSource;

/**
 * @author Bjoern Lohrmann, KNIME.com
 * @author Sascha Wolke, KNIME.com
 */
public class Spark_2_1_JavaSnippetHelper extends Spark_2_1_AbstractJavaSnippetHelper {

    private static final Class<?> INNER_SNIPPET_SUPERCLASS = AbstractSparkJavaSnippet.class;

    private static final Class<?> SOURCE_SNIPPET_SUPERCLASS = AbstractSparkJavaSnippetSource.class;

    private static final Class<?> SINK_SNIPPET_SUPERCLASS = AbstractSparkJavaSnippetSink.class;

    private final static String INNER_SNIPPET_METHOD_SIG =
        "public JavaRDD<Row> apply(final JavaSparkContext sc, final JavaRDD<Row> rowRDD1, final JavaRDD<Row> rowRDD2)"
            + " throws Exception";

    private final static String SOURCE_SNIPPET_METHOD_SIG =
        "public JavaRDD<Row> apply(final JavaSparkContext sc) throws Exception";

    private final static String SINK_SNIPPET_METHOD_SIG =
        "public void apply(final JavaSparkContext sc, final JavaRDD<Row> rowRDD) throws Exception";

    private final static String INNER_SNIPPET_DEFAULT_CONTENT = "return rowRDD1;";

    private final static String SINK_SNIPPET_DEFAULT_CONTENT = "// sink";

    private final static String SOURCE_SNIPPET_DEFAULT_CONTENT = "return sc.<Row>emptyRDD();";

    private final static String INNER_SNIPPET_CLASSNAME = "SparkJavaSnippet";

    private final static String SOURCE_SNIPPET_CLASSNAME = "SparkJavaSnippetSource";

    private final static String SINK_SNIPPET_CLASSNAME = "SparkJavaSnippetSink";

    @Override
    protected String[] getSystemImports() {
        return new String[]{"org.apache.spark.SparkContext", "org.apache.spark.api.java.JavaSparkContext",
            "org.apache.spark.api.java.*", "org.apache.spark.api.java.function.*", "org.apache.spark.sql.types.*","org.apache.spark.sql.*",
            "com.knime.bigdata.spark.core.exception.*",
            "com.knime.bigdata.spark2_1.api.RowBuilder",
            INNER_SNIPPET_SUPERCLASS.getName(),
            SOURCE_SNIPPET_SUPERCLASS.getName(), SINK_SNIPPET_SUPERCLASS.getName()};
    }

    @Override
    protected Class<?> getSnippetSuperClass(final SnippetType type) {
        switch (type) {
            case INNER:
                return INNER_SNIPPET_SUPERCLASS;
            case SOURCE:
                return SOURCE_SNIPPET_SUPERCLASS;
            case SINK:
                return SINK_SNIPPET_SUPERCLASS;
            default:
                throw new IllegalArgumentException("Unsupported snippet type: " + type.toString());
        }
    }

    @Override
    public String getSnippetClassName(final SnippetType type, final String suffix) {
        switch (type) {
            case INNER:
                return INNER_SNIPPET_CLASSNAME + suffix;
            case SOURCE:
                return SOURCE_SNIPPET_CLASSNAME + suffix;
            case SINK:
                return SINK_SNIPPET_CLASSNAME + suffix;
            default:
                throw new IllegalArgumentException("Unsupported snippet type: " + type.toString());
        }
    }

    @Override
    public String getDefaultContent(final SnippetType type) {
        switch (type) {
            case INNER:
                return INNER_SNIPPET_DEFAULT_CONTENT;
            case SOURCE:
                return SOURCE_SNIPPET_DEFAULT_CONTENT;
            case SINK:
                return SINK_SNIPPET_DEFAULT_CONTENT;
            default:
                throw new IllegalArgumentException("Unsupported snippet type: " + type.toString());
        }
    }

    @Override
    public String getMethodSignature(final SnippetType type) {
        switch (type) {
            case INNER:
                return INNER_SNIPPET_METHOD_SIG;
            case SOURCE:
                return SOURCE_SNIPPET_METHOD_SIG;
            case SINK:
                return SINK_SNIPPET_METHOD_SIG;
            default:
                throw new IllegalArgumentException("Unsupported snippet type: " + type.toString());
        }
    }
}
