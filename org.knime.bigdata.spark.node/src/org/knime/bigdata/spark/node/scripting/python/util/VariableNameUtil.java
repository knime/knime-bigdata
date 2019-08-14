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
 *   Created on 08.08.2019 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.scripting.python.util;

/**
 * This class handles the escaping of FlowVariable names for the PySpark scripts
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class VariableNameUtil {

    private VariableNameUtil() {
        //Utility Class
    }

    /**
     * Escapes the given variable name
     * @param name the name to escape
     * @return the escaped name
     */
    public static String escapeName(final String name) {
        return name.replaceAll("[^A-Za-z0-9_]", "_");
    }

}
