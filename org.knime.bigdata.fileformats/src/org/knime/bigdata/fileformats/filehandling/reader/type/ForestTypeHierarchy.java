/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Nov 13, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.type;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.knime.core.node.util.CheckUtils;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.NoCompatibleTypeException;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TraversableTypeHierarchy;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TreeTypeHierarchy;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TreeTypeHierarchy.TreeNode;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TypeHierarchy;

/**
 * A {@link TypeHierarchy} consisting of multiple {@link TreeTypeHierarchy} instances.</br>
 * The hierarchies are expected to be disjunct, i.e. no overlap between their types and at most one of them
 * {@link TypeHierarchy#supports(Object)} a given value.</br>
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <T> the type used to identify external types
 * @param <V> the type used for values
 */
public final class ForestTypeHierarchy<T, V>
    implements TypeHierarchy<T, V>, TraversableTypeHierarchy<T>, Iterable<TreeTypeHierarchy<T, V>> {

    private final List<TreeTypeHierarchy<T, V>> m_trees;

    ForestTypeHierarchy(final List<TreeTypeHierarchy<T, V>> trees) {
        checkDisjunct(trees);
        m_trees = new ArrayList<>(trees);
    }

    private static <T, V> void checkDisjunct(final List<TreeTypeHierarchy<T, V>> trees) {
        List<Set<T>> types = trees.stream()//
            .map(ForestTypeHierarchy::extractTypes)//
            .collect(toList());
        final int sizeOfUnionIfDisjunct = types.stream().mapToInt(Set::size).sum();
        final int sizeOfUnion = types.stream()//
            .reduce(ForestTypeHierarchy::addAllToFirst)//
            .orElseThrow(() -> new IllegalArgumentException("All hierarchies are empty."))//
            .size();
        CheckUtils.checkArgument(sizeOfUnionIfDisjunct == sizeOfUnion, "The provided hierarchies are not disjunct.");
    }

    private static <T> Set<T> addAllToFirst(final Set<T> first, final Set<T> second) {
        first.addAll(second);
        return first;
    }

    private static <T> Set<T> extractTypes(final TreeTypeHierarchy<T, ?> tree) {
        final Set<T> types = new HashSet<>();
        addTypesFromSubtree(types, tree.getRootNode());
        return types;
    }

    private static <T> void addTypesFromSubtree(final Set<T> types, final TreeNode<T, ?> node) {
        types.add(node.getType());
        for (TreeNode<T, ?> child : node.getChildren()) {
            addTypesFromSubtree(types, child);
        }
    }

    /**
     * {@inheritDoc}
     *
     * NOTE: The returned resolver will use the root type of the first tree supplied in the constructor if it never saw
     * any values.
     */
    @Override
    public TypeResolver<T, V> createResolver() {
        return new ForestTypeResolver();
    }

    @Override
    public boolean supports(final V value) {
        return m_trees.stream().anyMatch(t -> t.supports(value));
    }

    @Override
    public Iterator<TreeTypeHierarchy<T, V>> iterator() {
        return m_trees.iterator();
    }

    private class ForestTypeResolver implements TypeResolver<T, V> {

        private TypeResolver<T, V> m_typeResolver;

        private final Set<T> m_observedTypes = new HashSet<>();

        @Override
        public T getMostSpecificType() {
            if (m_typeResolver == null) {
                return m_trees.get(0).getRootNode().getType();
            } else {
                return m_typeResolver.getMostSpecificType();
            }
        }

        @Override
        public void accept(final V value) {
            if (m_typeResolver == null) {
                final List<TypeResolver<T, V>> resolvers = m_trees.stream()//
                    .filter(h -> h.supports(value))//
                    .map(TypeHierarchy::createResolver)//
                    .collect(toList());
                if (resolvers.isEmpty()) {
                    throw new NoCompatibleTypeException(value + " is not supported by any hierarchy.");
                }
                CheckUtils.checkState(resolvers.size() == 1, "More than one hierarchy supports the value '%s'.", value);
                m_typeResolver = resolvers.get(0);
            }
            try {
                m_typeResolver.accept(value);
                m_observedTypes.add(m_typeResolver.getMostSpecificType());
            } catch (NoCompatibleTypeException ex) {
                throw new NoCompatibleTypeException(
                    String.format("'%s' is not compatible with the type(s) %s.", value, m_observedTypes), ex);
            }
        }

        @Override
        public boolean reachedTop() {
            return hasType() && m_typeResolver.reachedTop();
        }

        @Override
        public boolean hasType() {
            return m_typeResolver != null;
        }

    }

    @Override
    public void traverseToRoot(final T startType, final Consumer<T> visitor) {
        for (TreeTypeHierarchy<T, V> tree : m_trees) {
            if (tree.contains(startType)) {
                tree.traverseToRoot(startType, visitor);
                return;
            }
        }
        throw new IllegalArgumentException("Unsupported startType encountered: " + startType);
    }

}
