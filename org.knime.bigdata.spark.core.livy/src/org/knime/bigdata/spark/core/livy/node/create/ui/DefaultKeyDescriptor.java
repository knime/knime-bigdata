package org.knime.bigdata.spark.core.livy.node.create.ui;

/**
 * Simple default implementation of {@link KeyDescriptor}.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 *
 */
public class DefaultKeyDescriptor implements KeyDescriptor {

    private final String m_key;

    private final String m_defaultValue;

    private final boolean m_nullable;

    private final String m_description;

    /**
     * Constructor for key descriptor (defaults to non-nullable).
     * 
     * @param key The key to describe.
     * @param defaultValue The default value for key-value pairs with the given key.
     * @param description A description of what the key means.
     */
    public DefaultKeyDescriptor(final String key, final String defaultValue, final String description) {
        this(key, defaultValue, false, description);
    }

    /**
     * Constructor.
     * 
     * @param key The key to describe.
     * @param defaultValue The default value for key-value pairs with the given key.
     * @param nullable Whether the values assigned to the key may be null or not.
     * @param description A description of what the key means.
     */
    public DefaultKeyDescriptor(final String key, final String defaultValue, final boolean nullable,
        final String description) {

        m_key = key;
        m_defaultValue = defaultValue;
        m_nullable = nullable;
        m_description = description;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getKey() {
        return m_key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDefaultValue() {
        return m_defaultValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNullable() {
        return m_nullable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return m_description;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(KeyDescriptor o) {
        final int keyCmp = m_key.compareTo(o.getKey());
        if (keyCmp != 0) {
            return keyCmp;
        }

        final int defaultValueCmp = m_defaultValue.compareTo(o.getDefaultValue());
        if (defaultValueCmp != 0) {
            return defaultValueCmp;
        }

        final int nullableCmp = Boolean.compare(m_nullable, o.isNullable());
        if (nullableCmp != 0) {
            return nullableCmp;
        }

        final int toolTipCmp = m_description.compareTo(o.getDescription());
        if (toolTipCmp != 0) {
            return toolTipCmp;
        }

        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_defaultValue == null) ? 0 : m_defaultValue.hashCode());
        result = prime * result + ((m_key == null) ? 0 : m_key.hashCode());
        result = prime * result + (m_nullable ? 1231 : 1237);
        result = prime * result + ((m_description == null) ? 0 : m_description.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DefaultKeyDescriptor other = (DefaultKeyDescriptor)obj;
        if (m_defaultValue == null) {
            if (other.m_defaultValue != null)
                return false;
        } else if (!m_defaultValue.equals(other.m_defaultValue))
            return false;
        if (m_key == null) {
            if (other.m_key != null)
                return false;
        } else if (!m_key.equals(other.m_key))
            return false;
        if (m_nullable != other.m_nullable)
            return false;
        if (m_description == null) {
            if (other.m_description != null)
                return false;
        } else if (!m_description.equals(other.m_description))
            return false;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateValue(String value) throws IllegalArgumentException {
        if (!m_nullable && (value == null || value.isEmpty())) {
            throw new IllegalArgumentException("Value must not be empty.");
        }
    }
}
