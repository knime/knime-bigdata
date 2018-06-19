package org.knime.bigdata.spark.core.livy.node.create.ui;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.swing.event.ChangeListener;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.config.base.ConfigStringEntry;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;

/**
 * A settings model that holds and <i>ordered</i> list of key-value pairs (assignments). The possible assignments are
 * limited to a <i>fixed</i> set of available keys. The available keys are described using {@link KeyDescriptor}s. The
 * key-value pairs are ordered, thus they can be accessed either by key or index.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SettingsModelKeyValue extends SettingsModel {

    private final String m_configName;

    private final ArrayList<String> m_assignedKeys = new ArrayList<>();

    private final TreeSet<String> m_unassignedKeys = new TreeSet<>();

    private final Map<String, String> m_assignments = new HashMap<>();

    private final Map<String, KeyDescriptor> m_supportedKeys = new LinkedHashMap<>();

    /**
     * Constructor.
     * 
     * @param configName The name of the node settings key.
     * @param keys A list of supported keys.
     */
    public SettingsModelKeyValue(String configName, List<KeyDescriptor> keys) {
        super();
        m_configName = configName;

        // add them in the same order as provided (this is important for addNext()).
        for (KeyDescriptor supportedKey : keys) {
            m_supportedKeys.put(supportedKey.getKey(), supportedKey);
        }
        m_unassignedKeys.addAll(m_supportedKeys.keySet());
    }

    /**
     * Takes the next unassigned key and assigns the key's default value.
     * 
     * @return the key that has been assigned.
     */
    public String addNext() {
        final String nextKey = m_unassignedKeys.first();
        setKeyValuePair(nextKey, m_supportedKeys.get(nextKey).getDefaultValue());
        return nextKey;
    }

    /**
     * @return whether there are still unassigned keys or not.
     */
    public boolean hasNext() {
        return !m_unassignedKeys.isEmpty();
    }

    /**
     * Takes all unassigned keys and assigns to each its respective default value.
     */
    public void addAll() {
        for (String key : m_unassignedKeys) {
            m_assignedKeys.add(key);
            m_assignments.put(key, m_supportedKeys.get(key).getDefaultValue());
        }
        m_unassignedKeys.clear();
        notifyChangeListeners();
    }

    /**
     * Clears all previous key-value assignments and replaces them with the given ones. The new assignments are done in
     * the order of the iterator of the given map.
     * 
     * @param newAssignments A map of new key-value pairs.
     * @throws IllegalArgumentException If the given map contains unsupported keys.
     */
    public void setKeyValuePairs(Map<String, String> newAssignments) {
        newAssignments.keySet().stream().forEach(this::ensureKeySupported);

        setKeyValuePairsInternal(newAssignments);
        notifyChangeListeners();
    }

    private void setKeyValuePairsInternal(Map<String, String> newAssignments) {
        m_assignedKeys.clear();
        m_assignments.clear();
        m_unassignedKeys.addAll(m_supportedKeys.keySet());

        m_assignedKeys.addAll(newAssignments.keySet());
        m_assignments.putAll(newAssignments);
        m_unassignedKeys.removeAll(newAssignments.keySet());
    }

    /**
     * Assigns the given value to the given key. If the key was previously unassigned, the assignment will be added to
     * the end of the list of assignments.
     * 
     * @param key The key to assign to.
     * @param newValue The value to assign.
     * @throws IllegalArgumentException if the given key is not supported.
     */
    public void setKeyValuePair(String key, String newValue) {
        ensureKeySupported(key);

        final boolean previouslyUnassigned = !m_assignments.containsKey(key);
        final boolean changed = previouslyUnassigned || !Objects.equals(m_assignments.get(key), newValue);

        if (previouslyUnassigned) {
            m_assignedKeys.add(key);
        }
        m_assignments.put(key, newValue);
        m_unassignedKeys.remove(key);

        if (changed) {
            notifyChangeListeners();
        }
    }

    /**
     * Replaces an existing assignment with a new one. The position of the new assignment will be identical to that of
     * the old assignment.
     * 
     * @param prevKey The old key to unassign.
     * @param newKey The new key to assign to.
     * @param newValue The value to assign to newKey.
     * @throws IllegalArgumentException if oldKey was currently unassigned, or if newKey is already assigned, or if
     *             newKey is not a supported key.
     */
    public void replaceKeyValuePair(String prevKey, String newKey, String newValue) {
        if (!m_assignments.containsKey(prevKey)) {
            throw new IllegalArgumentException("Key is currently unassigned: " + prevKey);
        }

        if (m_assignments.containsKey(newKey)) {
            throw new IllegalArgumentException("Key is already assigned: " + newKey);
        }

        ensureKeySupported(newKey);

        int preKeyIdx = m_assignedKeys.indexOf(prevKey);
        m_assignedKeys.set(preKeyIdx, newKey);
        m_assignments.remove(prevKey);
        m_assignments.put(newKey, newValue);
        m_unassignedKeys.add(prevKey);
        m_unassignedKeys.remove(newKey);

        notifyChangeListeners();
    }

    /**
     * Unassigns the given key.
     * 
     * @param key The key to unassign.
     */
    public void removeKey(String key) {
        if (m_assignments.containsKey(key)) {
            m_assignedKeys.remove(key);
            m_assignments.remove(key);
            m_unassignedKeys.add(key);
            notifyChangeListeners();
        }
    }

    /**
     * Unassigns the key at the given index position.
     * 
     * @param index The index (indices start at 0).
     * @return The now unassigned key.
     */
    public String removeKey(int index) {
        final String keyToRemove = m_assignedKeys.remove(index);
        m_assignments.remove(keyToRemove);
        m_unassignedKeys.add(keyToRemove);

        notifyChangeListeners();

        return keyToRemove;
    }

    private void ensureKeySupported(String key) {
        if (!isSupported(key)) {
            throw new IllegalArgumentException("Unsupported key: " + key);
        }
    }

    /**
     * Checks whether the given key is supported or not.
     * 
     * @param key The key to check.
     * @return whether the given key is supported.
     */
    public boolean isSupported(String key) {
        return m_supportedKeys.containsKey(key);
    }

    /**
     * Clears all key-value assignments.
     */
    public void clear() {
        m_assignments.clear();
        m_assignedKeys.clear();
        m_unassignedKeys.addAll(m_supportedKeys.keySet());
        notifyChangeListeners();
    }

    /**
     * Gets the value currently assigned to the given key.
     * 
     * @param key The key get the assigned value for.
     * @return the value assigned to the given key.
     */
    public String getValue(String key) {
        return m_assignments.get(key);
    }

    /**
     * Gets the key at the given index.
     * 
     * @param index The index position to get the key for.
     * @return the key at the given index position (indices start at 0).
     */
    public String getKey(int index) {
        return m_assignedKeys.get(index);
    }

    /**
     * @return a list of the assigned keys.
     */
    public List<String> getAssignedKeys() {
        return Collections.unmodifiableList(m_assignedKeys);
    }

    /**
     * @return a ordered set of the already assigned keys.
     */
    public SortedSet<String> getUnassignedKeys() {
        return new TreeSet<>(m_unassignedKeys);
    }

    /**
     * Checks whether a value is assigned to the given key.
     * 
     * @param key Key to check whether it is assigned or not.
     * @return true when the key has a value assigned, false otherwise.
     */
    public boolean isAssignedKey(String key) {
        return m_assignments.containsKey(key);
    }

    /**
     * @return a map containing all the key-value assignments.
     */
    public Map<String, String> getKeyValuePairs() {
        return Collections.unmodifiableMap(m_assignments);
    }

    /**
     * @return a list of all the supported keys as {@link KeyDescriptor}s.
     */
    public List<KeyDescriptor> getSupportedKeys() {
        return new ArrayList<>(m_supportedKeys.values());
    }

    /**
     * Gets the descriptor for the given key.
     * 
     * @param key The key to get the {@link KeyDescriptor} for.
     * @return the {@link KeyDescriptor} for the given key, or null if the key is not supported.
     */
    public KeyDescriptor getSupportedKey(String key) {
        return m_supportedKeys.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prependChangeListener(ChangeListener l) {
        super.prependChangeListener(l);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    protected SettingsModelKeyValue createClone() {
        final SettingsModelKeyValue clone = new SettingsModelKeyValue(m_configName, getSupportedKeys());
        clone.m_assignedKeys.addAll(m_assignedKeys);
        clone.m_assignments.putAll(m_assignments);
        clone.m_unassignedKeys.removeAll(m_assignedKeys);
        return clone;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getModelTypeID() {
        return "SMID_keyValueSettings";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getConfigName() {
        return m_configName;
    }

    /**
     * Validates all key-value assignments in the model.
     * 
     * @throws InvalidSettingsException if a key-value assignment failed to validate.
     */
    public void validate() throws InvalidSettingsException {
        for (String key : m_assignedKeys) {
            validateAssignmentForKey(key, m_assignments.get(key));
        }
    }

    private void validateAssignmentForKey(String key, String value) throws InvalidSettingsException {
        try {
            ensureKeySupported(key);
            m_supportedKeys.get(key).validateValue(value);

        } catch (IllegalArgumentException e) {
            throw new InvalidSettingsException(String.format("Invalid value for %s: %s", key, e.getMessage()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsForDialog(NodeSettingsRO settings, PortObjectSpec[] specs)
        throws NotConfigurableException {

        try {
            loadSettingsForModel(settings);
        } catch (InvalidSettingsException e) {
            if (isEnabled()) {
                throw (new NotConfigurableException(e.getMessage()));
            }
        } finally {
            // always notify the listeners. That is, because there could be an
            // invalid value displayed in the listener.
            notifyChangeListeners();
        }
    }

    /**
     * Reads data from the given settings object.
     * 
     * @param settings The settings object to read from.
     * @return A map with the extracted key-value pairs.
     * @throws InvalidSettingsException if a key was found twice in the settings object.
     */
    protected LinkedHashMap<String, String> extractDataFromSettings(NodeSettingsRO settings)
        throws InvalidSettingsException {

        final LinkedHashMap<String, String> data = new LinkedHashMap<>();

        @SuppressWarnings("unchecked")
        final Enumeration<ConfigStringEntry> rows = settings.getNodeSettings(getConfigName()).children();
        while (rows.hasMoreElements()) {
            final ConfigStringEntry row = rows.nextElement();
            if (data.put(row.getKey(), row.getString()) != null) {
                throw new InvalidSettingsException("Duplicate key: " + row.getKey());
            }
        }

        return data;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsForDialog(NodeSettingsWO settings) throws InvalidSettingsException {
        saveSettingsForModel(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettingsForModel(NodeSettingsRO settings) throws InvalidSettingsException {
        Map<String, String> data = extractDataFromSettings(settings);

        for (String key : data.keySet()) {
            validateAssignmentForKey(key, data.get(key));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsForModel(NodeSettingsRO settings) throws InvalidSettingsException {
        if (!settings.containsKey(getConfigName())) {
            throw new InvalidSettingsException("No settings defined");
        }

        setKeyValuePairsInternal(extractDataFromSettings(settings));
        notifyChangeListeners();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsForModel(NodeSettingsWO settings) {
        final NodeSettingsWO settingsData = settings.addNodeSettings(getConfigName());

        for (String key : m_assignments.keySet()) {
            settingsData.addString(key, m_assignments.get(key));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + " ('" + m_configName + "')";
    }
}
