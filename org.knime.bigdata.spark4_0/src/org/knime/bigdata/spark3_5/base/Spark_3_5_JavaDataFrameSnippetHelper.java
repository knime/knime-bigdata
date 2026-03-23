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
 *   Created on May 6, 2016 by bjoern
 */
package org.knime.bigdata.spark3_5.base;

import org.knime.bigdata.spark3_5.jobs.scripting.java.AbstractSparkDataFrameJavaSnippet;
import org.knime.bigdata.spark3_5.jobs.scripting.java.AbstractSparkDataFrameJavaSnippetSink;
import org.knime.bigdata.spark3_5.jobs.scripting.java.AbstractSparkDataFrameJavaSnippetSource;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark_3_5_JavaDataFrameSnippetHelper extends Spark_3_5_AbstractJavaSnippetHelper {

    private static final Class<?> INNER_SNIPPET_SUPERCLASS = AbstractSparkDataFrameJavaSnippet.class;

    private static final Class<?> SOURCE_SNIPPET_SUPERCLASS = AbstractSparkDataFrameJavaSnippetSource.class;

    private static final Class<?> SINK_SNIPPET_SUPERCLASS = AbstractSparkDataFrameJavaSnippetSink.class;

    private final static String INNER_SNIPPET_METHOD_SIG =
        "public Dataset<Row> apply(final SparkSession spark, final Dataset<Row> dataFrame1, final Dataset<Row> dataFrame2)"
            + " throws Exception";

    private final static String SOURCE_SNIPPET_METHOD_SIG =
        "public Dataset<Row> apply(final SparkSession spark) " + "throws Exception";

    private final static String SINK_SNIPPET_METHOD_SIG =
        "public void apply(final SparkSession spark, final Dataset<Row> dataFrame)" + " throws Exception";

    private final static String INNER_SNIPPET_DEFAULT_CONTENT = "return dataFrame1;";

    private final static String SINK_SNIPPET_DEFAULT_CONTENT = "// sink";

    private final static String SOURCE_SNIPPET_DEFAULT_CONTENT = "return spark.emptyDataFrame();";

    private final static String INNER_SNIPPET_CLASSNAME = "SparkDataFrameJavaSnippet";

    private final static String SOURCE_SNIPPET_CLASSNAME = "SparkDataFrameJavaSnippetSource";

    private final static String SINK_SNIPPET_CLASSNAME = "SparkDataFrameJavaSnippetSink";

    @Override
    protected String[] getSystemImports() {
        return new String[]{"org.apache.spark.SparkContext", "org.apache.spark.api.java.JavaSparkContext",
            "org.apache.spark.api.java.*", "org.apache.spark.api.java.function.*",
            "org.apache.spark.sql.types.*", "org.apache.spark.sql.*", "static org.apache.spark.sql.functions.*",
            "org.knime.bigdata.spark.core.exception.*",
            "org.knime.bigdata.spark3_5.api.RowBuilder",
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
