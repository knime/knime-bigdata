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

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Ole Ostergaard, KNIME.com GmbH, Konstanz, Germany
 */
@SparkClass
public class CheckJarPresenceJobInput extends JobInput {

    private static final String JAR_CLASS_LIST = "jarClassList";

    /** Parameless constructor for automatic deserialization **/
    public CheckJarPresenceJobInput() {}

    /**
     * @param jarClassList
     */
    public CheckJarPresenceJobInput(final List<String> jarClassList) {
        set(JAR_CLASS_LIST, jarClassList);
    }

    /**
     * Returns the list of jar classes.
     *
     * @return the list of jar classes
     */
    public List<String> getJarClassList() {
        return get(JAR_CLASS_LIST);
    }

}
