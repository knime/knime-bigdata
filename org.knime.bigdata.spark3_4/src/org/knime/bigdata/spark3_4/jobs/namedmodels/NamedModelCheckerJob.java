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
package org.knime.bigdata.spark3_4.jobs.namedmodels;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.model.MissingNamedModelException;
import org.knime.bigdata.spark.core.model.NamedModelCheckerJobInput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SimpleSparkJob;

/**
 * Helper job to manage named objects on the server side.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class NamedModelCheckerJob implements SimpleSparkJob<NamedModelCheckerJobInput> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = Logger.getLogger(NamedModelCheckerJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final NamedModelCheckerJobInput input,
        final NamedObjects namedObjects) throws Exception {

        if (!namedObjects.validateNamedObject(input.getNamedModelId())) {
            LOG.info(String.format("Named model with ID %s not found", input.getNamedModelId()));
            throw new MissingNamedModelException(input.getNamedModelId());
        }
    }
}
