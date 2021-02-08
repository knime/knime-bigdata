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
 *   Created on Jul 4, 2018 by bjoern
 */
package org.knime.bigdata.spark3_0.jobs.prepare.databricks;

import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded;
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved;
import org.apache.spark.scheduler.SparkListenerBlockUpdated;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorBlacklisted;
import org.apache.spark.scheduler.SparkListenerExecutorBlacklistedForStage;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerExecutorUnblacklisted;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerNodeBlacklisted;
import org.apache.spark.scheduler.SparkListenerNodeBlacklistedForStage;
import org.apache.spark.scheduler.SparkListenerNodeUnblacklisted;
import org.apache.spark.scheduler.SparkListenerSpeculativeTaskSubmitted;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageExecutorMetrics;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskGettingResult;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksSparkSideStagingArea;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Spark listener that cleans up the staging area and local temp files when the Spark context ends.
 *
 * @author Bjoern Lohrmann, KIME GmbH
 */
@SparkClass
public class KNIMEDatabricksSparkListener implements SparkListenerInterface {

    /**
     * {@inheritDoc}
     */
    @Override
    public void onApplicationEnd(final SparkListenerApplicationEnd endEvent) {
        DatabricksSparkSideStagingArea.SINGLETON_INSTANCE.cleanUp();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onApplicationStart(final SparkListenerApplicationStart arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onBlockManagerAdded(final SparkListenerBlockManagerAdded arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onBlockManagerRemoved(final SparkListenerBlockManagerRemoved arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onBlockUpdated(final SparkListenerBlockUpdated arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onEnvironmentUpdate(final SparkListenerEnvironmentUpdate arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onExecutorAdded(final SparkListenerExecutorAdded arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onExecutorBlacklisted(final SparkListenerExecutorBlacklisted arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onExecutorMetricsUpdate(final SparkListenerExecutorMetricsUpdate arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onExecutorRemoved(final SparkListenerExecutorRemoved arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onExecutorUnblacklisted(final SparkListenerExecutorUnblacklisted arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onJobEnd(final SparkListenerJobEnd arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onJobStart(final SparkListenerJobStart arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onNodeBlacklisted(final SparkListenerNodeBlacklisted arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onNodeUnblacklisted(final SparkListenerNodeUnblacklisted arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onOtherEvent(final SparkListenerEvent arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onStageCompleted(final SparkListenerStageCompleted arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onStageSubmitted(final SparkListenerStageSubmitted arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTaskEnd(final SparkListenerTaskEnd arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTaskGettingResult(final SparkListenerTaskGettingResult arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTaskStart(final SparkListenerTaskStart arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onUnpersistRDD(final SparkListenerUnpersistRDD arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onSpeculativeTaskSubmitted(final SparkListenerSpeculativeTaskSubmitted arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onExecutorBlacklistedForStage(final SparkListenerExecutorBlacklistedForStage arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onNodeBlacklistedForStage(final SparkListenerNodeBlacklistedForStage arg0) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onStageExecutorMetrics(final SparkListenerStageExecutorMetrics executorMetrics) {
    }
}
