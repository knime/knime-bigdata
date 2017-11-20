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
package org.knime.bigdata.spark.node.scripting.java.util.helper;

/**
 * Extension point registry for {@link JavaSnippetHelper}s.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class JavaSnippetHelperRegistry extends AbstractJavaSnippetHelperRegistry {

    /** The id of the converter extension point. */
    private static final String EXT_POINT_ID = "org.knime.bigdata.spark.node.JavaSnippetHelper";

    /**The attribute of the extension point.*/
    private static final String EXT_POINT_ATTR_DF = "HelperClass";

    private static JavaSnippetHelperRegistry instance;

    private JavaSnippetHelperRegistry() {
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public static synchronized JavaSnippetHelperRegistry getInstance() {
        if (instance == null) {
            instance = new JavaSnippetHelperRegistry();
            instance.registerExtensions(EXT_POINT_ID, EXT_POINT_ATTR_DF);
        }
        return instance;
    }
}
