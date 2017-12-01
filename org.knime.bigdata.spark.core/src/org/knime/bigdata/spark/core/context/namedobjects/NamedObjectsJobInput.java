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
 *   Created on 08.02.2016 by koetter
 */
package org.knime.bigdata.spark.core.context.namedobjects;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * {@link JobInput} implementation to work with named objects e.g. list or delete.
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class NamedObjectsJobInput extends JobInput {

    private final static String KEY_NAMED_OBJECTS_OP = "operation";

    private final static String KEY_NAMED_OBJECTS_TO_DELETE = "namedObjectsToDelete";

    /**
     * The available named objects operations.
     */
    public enum Operation {
            LIST, DELETE;
    }

    /**
     *
     */
    public NamedObjectsJobInput() {
        super();
    }

    /**
     * @return the named objects operation to execute
     * @see Operation
     */
    public Operation getOperation() {
        final String op = get(KEY_NAMED_OBJECTS_OP);
        return Operation.valueOf(op);
    }

    /**
     * @return the unique names of the named objects to delete
     */
    public Set<String> getNamedObjectsToDelete() {
        return new HashSet<>(getOrDefault(KEY_NAMED_OBJECTS_TO_DELETE, Collections.<String> emptyList()));
    }

    /**
     * @return {@link NamedObjectsJobInput}
     */
    public static NamedObjectsJobInput createListOperation() {
        NamedObjectsJobInput toReturn = new NamedObjectsJobInput();
        toReturn.set(KEY_NAMED_OBJECTS_OP, Operation.LIST.toString());
        return toReturn;
    }

    /**
     * @param namedObjectIds {@link Set} with the unique names of the named objects to delete
     * @return {@link NamedObjectsJobInput} to delete the given named objects
     */
    public static NamedObjectsJobInput createDeleteOperation(final Set<String> namedObjectIds) {
        NamedObjectsJobInput toReturn = new NamedObjectsJobInput();
        toReturn.set(KEY_NAMED_OBJECTS_OP, Operation.DELETE.toString());
        toReturn.set(KEY_NAMED_OBJECTS_TO_DELETE, new ArrayList<>(namedObjectIds));
        return toReturn;
    }
}
