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
 *   Created on Sep 21, 2017 by bjoern
 */
package com.knime.bigdata.spark.node.knosp;

import org.knime.core.node.streamable.StreamableOperator;
import org.osgi.framework.Version;

import com.knime.bigdata.spark.node.io.table.reader.AbstractTable2SparkStreamableOperator;
import com.knime.bigdata.spark.node.io.table.reader.Table2SparkNodeModel;
import com.knime.bigdata.spark.node.io.table.writer.AbstractSpark2TableStreamableOperator;
import com.knime.bigdata.spark.node.io.table.writer.Spark2TableNodeModel;

/**
 * KNIME-on-Spark (KNOSP) helper interface. This is used by the {@link Table2SparkNodeModel} and
 * {@link Spark2TableNodeModel} to obtain a {@link StreamableOperator} that ships data between plain-java Spark and the
 * KNOSP Executor running within OSGI.
 * <p/>
 * An implementation of this interface is registered as an OSGI service by the KNIME-on-Spark extension.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public interface KNOSPHelper {

    /**
     * Provides a {@link StreamableOperator} that streams rows from inside the KNOSP Executor (OSGI) to plain-java
     * Spark, all within the same JVM.
     *
     * @param knospOutputID An ID under which the streamable operator should register itself (this ID is looked up
     *            inside Spark).
     * @param knimeSparkExecutorVersion The version of KNIME Spark Executor to assume when mapping types and values
     *            between the KNIME and intermediate type domains.
     * @return A {@link AbstractTable2SparkStreamableOperator} that streams rows to Spark within the same JVM.
     */
    public AbstractTable2SparkStreamableOperator createTable2SparkStreamableOperator(final String knospOutputID,
        final Version knimeSparkExecutorVersion);

    /**
     * Provides a {@link StreamableOperator} that streams rows from plain-java Spark to the KNOSP Executor, all within
     * the same JVM.
     *
     * @param knospInputID An ID the streamable operator should look up to get a handle on data that is being streamed
     *            in from a Spark RDD/Dataset partition.
     * @return A {@link AbstractSpark2TableStreamableOperator} that streams rows from plain-java Spark within the same
     *         JVM.
     */
    public AbstractSpark2TableStreamableOperator createSpark2TableStreamableOperator(final String knospInputID);

}
