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
 *   Created on 22.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.scripting.python.util;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Class for JobOutput fpr PySpark nodes
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
@SparkClass
public class PySparkJobOutput extends JobOutput {

    /**
     * Empty constructor for serialization and for cases where the script does not create a named output object.
     */
    public PySparkJobOutput() {
    }

    /**
     * Constructor used to return a spec back to the client
     *
     * @param namedOutputObject the input objects names
     * @param outputSchema the
     */
    public PySparkJobOutput(final String[] namedOutputObject, final IntermediateSpec[] outputSchema) {
        if (namedOutputObject.length != outputSchema.length) {
            throw new IllegalStateException("Output objects and schema size do not match");
        }
        for (int i = 0; i < namedOutputObject.length; i++) {
            withSpec(namedOutputObject[i], outputSchema[i]);
        }
    }

}
