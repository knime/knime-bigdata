package org.knime.bigdata.spark.core.livy.node.create.ui;

/**
 * Describes a string-based key-value pair. i.e. the key, a default value, a description text as well as value
 * validation logic (see {@link #validateValue(String)}). Instances of this class are not meant to actually hold a
 * concrete association of key and value, but only provide meta-data and validation logic.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public interface KeyDescriptor extends Comparable<KeyDescriptor>, Cloneable {
    
    /**
     * @return a key that uniquely identifies the key-value property.
     */
    String getKey();

    /**
     * @return a default value for key-value pairs with the key.
     */
    String getDefaultValue();

    /**
     * @return a user-friendly description of what the key-value property means.
     */
    String getDescription();
    
    /**
     * Validates that the given value is valid for this key-value property and throws an exception if not.
     * 
     * @param value A value to validate.
     * @throws IllegalArgumentException if validation failed.
     */
    void validateValue(String value) throws IllegalArgumentException;

    /**
     * @return whether null or empty values are allowed
     */
    boolean isNullable();
}
