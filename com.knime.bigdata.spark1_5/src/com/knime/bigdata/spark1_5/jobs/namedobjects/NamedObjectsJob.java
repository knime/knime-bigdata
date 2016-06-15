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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark1_5.jobs.namedobjects;

import java.util.Set;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;

import com.knime.bigdata.spark.core.context.namedobjects.NamedObjectsJobInput;
import com.knime.bigdata.spark.core.context.namedobjects.NamedObjectsJobOutput;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark1_5.api.NamedObjects;
import com.knime.bigdata.spark1_5.api.SparkJob;

/**
 * Helper job to manage named objects on the server side.
 *
 * @author dwk
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class NamedObjectsJob implements SparkJob<NamedObjectsJobInput, NamedObjectsJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(NamedObjectsJob.class.getName());

    @Override
    public NamedObjectsJobOutput runJob(final SparkContext sparkContext, final NamedObjectsJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        switch (input.getOperation()) {
            case DELETE:
                deleteNamedRDDs(input.getNamedObjectsToDelete(), namedObjects);
                return NamedObjectsJobOutput.createDeleteObjectsSuccess();
            case LIST:
                return NamedObjectsJobOutput.createListObjectsSuccess(namedObjects.getNamedObjects());
            default:
                throw new Exception("Unsupported operation on named objects: " + input.getOperation().toString());
        }
    }

    private void deleteNamedRDDs(final Set<String> namedObjectsToDelete, final NamedObjects namedObjects) {

        for (String rddName : namedObjectsToDelete) {
            if (namedObjects.validateNamedObject(rddName)) {
                LOGGER.info("Deleting reference to named RDD " + rddName);
                namedObjects.deleteNamedObject(rddName);
            }

            if (namedObjects.validateNamedObject(rddName)) {
                LOGGER.warning("Failed to delete reference to named RDD " + rddName);
            }
        }

    }
}
