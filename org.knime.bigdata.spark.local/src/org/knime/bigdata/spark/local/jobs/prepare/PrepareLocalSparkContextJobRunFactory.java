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
 *   Created on Apr 27, 2016 by bjoern
 */
package org.knime.bigdata.spark.local.jobs.prepare;

import org.knime.bigdata.spark.core.context.util.PrepareContextJobInput;
import org.knime.bigdata.spark.core.job.DefaultSimpleJobRunFactory;
import org.knime.bigdata.spark.local.context.LocalSparkContext;

/**
 * Job run factory for the {@link PrepareLocalSparkContextJob}.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class PrepareLocalSparkContextJobRunFactory extends DefaultSimpleJobRunFactory<PrepareContextJobInput> {

	/**
	 * Constructor.
	 */
	public PrepareLocalSparkContextJobRunFactory() {
		super(LocalSparkContext.PREPARE_LOCAL_SPARK_CONTEXT_JOB, PrepareLocalSparkContextJob.class);
	}
}
