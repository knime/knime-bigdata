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
 *   Created on May 4, 2018 by oole
 */
package org.knime.bigdata.spark.core.util.jarupload;

import java.util.List;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Ole Ostergaard, KNIME GmbH, Konstanz, Germany
 */
@SparkClass
public class CheckJarPresenceJobOutput extends JobOutput {

    private static final String MISSING_JAR_CLASS_LIST = "missingJarClassList";

    /** Paramless constructor for automatic deserialization **/
    public CheckJarPresenceJobOutput() {}


    /**
     * @param missingJarClassList The list of missing jar classes
     */
    public CheckJarPresenceJobOutput(final List<String> missingJarClassList) {
        set(MISSING_JAR_CLASS_LIST, missingJarClassList);
    }

    /**
     * Returns the list of missing jar classes.
     *
     * @return The list of missing jar classes
     */
    public List<String> getMissingJarClassList() {
        return get(MISSING_JAR_CLASS_LIST);
    }
}
