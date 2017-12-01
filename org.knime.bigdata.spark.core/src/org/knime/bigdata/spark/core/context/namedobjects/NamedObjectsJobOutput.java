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
 *   Created on Apr 12, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.context.namedobjects;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * {@link JobOutput} implementation to work with named objects e.g. list or delete.
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class NamedObjectsJobOutput extends JobOutput {

    private static final String KEY_LIST_OF_NAMED_OBJECTS = "namedObjectsList";

    private static final String KEY_DELETE_SUCCESS = "deleteSuccess";

    /**
     *
     */
    public NamedObjectsJobOutput() {
        super();
    }

    /**
     * @return {@link Set} with the unique names of the named objects
     */
    public Set<String> getListOfNamedObjects() {
        return new HashSet<>(getOrDefault(KEY_LIST_OF_NAMED_OBJECTS, Collections.<String> emptyList()));
    }

    /**
     * @return <code>true</code> if delete job succceeded
     */
    public boolean isDeleteSuccess() {
        return getOrDefault(KEY_DELETE_SUCCESS, false);
    }

    /**
     * @return creates the {@link NamedObjectsJobOutput} for a successful deletion
     */
    public static NamedObjectsJobOutput createDeleteObjectsSuccess() {
        NamedObjectsJobOutput deleteSuccess = new NamedObjectsJobOutput();
        deleteSuccess.set(KEY_DELETE_SUCCESS, true);
        return deleteSuccess;
    }

    /**
     * @param namedObjects {@link Set} with the unique names of the named objects that where successful deleted
     * @return {@link NamedObjectsJobOutput}
     */
    public static NamedObjectsJobOutput createListObjectsSuccess(final Set<String> namedObjects) {
        NamedObjectsJobOutput listSuccess = new NamedObjectsJobOutput();
        listSuccess.set(KEY_LIST_OF_NAMED_OBJECTS, new ArrayList<>(namedObjects));
        return listSuccess;
    }
}
