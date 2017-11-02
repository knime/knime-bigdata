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
package com.knime.bigdata.spark.core.context.namedobjects;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class NamedObjectsJobOutput extends JobOutput {

    private static final String KEY_LIST_OF_NAMED_OBJECTS = "namedObjectsList";

    private static final String KEY_DELETE_SUCCESS = "deleteSuccess";

    public NamedObjectsJobOutput() {
        super();
    }

    public Set<String> getListOfNamedObjects() {
        return new HashSet<String>(getOrDefault(KEY_LIST_OF_NAMED_OBJECTS, Collections.<String> emptyList()));
    }

    public boolean isDeleteSuccess() {
        return getOrDefault(KEY_DELETE_SUCCESS, false);
    }

    public static NamedObjectsJobOutput createDeleteObjectsSuccess() {
        NamedObjectsJobOutput deleteSuccess = new NamedObjectsJobOutput();
        deleteSuccess.set(KEY_DELETE_SUCCESS, true);
        return deleteSuccess;
    }

    public static NamedObjectsJobOutput createListObjectsSuccess(final Set<String> namedObjects) {
        NamedObjectsJobOutput listSuccess = new NamedObjectsJobOutput();
        listSuccess.set(KEY_LIST_OF_NAMED_OBJECTS, new ArrayList<String>(namedObjects));
        return listSuccess;
    }
}
