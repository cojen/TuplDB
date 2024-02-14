/*
 *  Copyright (C) 2024 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.table;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;

/**
 * Replacement for Collections.emptyNavigableSet which doesn't throw a ClassCastException (to
 * Comparable) when passing non-Comparable instances into it.
 *
 * @author Brian S. O'Neill
 */
public final class EmptyNavigableSet<E> extends AbstractSet<E> implements NavigableSet<E> {
    private static final EmptyNavigableSet THE = new EmptyNavigableSet();

    @SuppressWarnings("unchecked")
    public static <E> EmptyNavigableSet<E> the() {
        return THE;
    }

    private EmptyNavigableSet() {
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Object[] toArray() {
        return RowUtils.NO_ARGS;
    }

    @Override
    public boolean contains(Object obj) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return c.isEmpty();
    }

    @Override
    public boolean remove(Object obj) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {
    }

    @Override
    public E lower(E e) {
        return null;
    }

    @Override
    public E floor(E e) {
        return null;
    }

    @Override
    public E ceiling(E e) {
        return null;
    }

    @Override
    public E higher(E e) {
        return null;
    }

    @Override
    public E pollFirst() {
        return null;
    }

    @Override
    public E first() {
        throw new NoSuchElementException();
    }

    @Override
    public E pollLast() {
        return null;
    }

    @Override
    public E last() {
        throw new NoSuchElementException();
    }

    @Override
    public Comparator<? super E> comparator() {
        return null;
    }

    @Override
    public Iterator<E> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public NavigableSet<E> descendingSet() {
        return this;
    }

    @Override
    public Iterator<E> descendingIterator() {
        return Collections.emptyIterator();
    }

    @Override
    public NavigableSet<E> subSet(E fromElement, E toElement) {
        return this;
    }

    @Override
    public NavigableSet<E> subSet(E fromElement, boolean fromInclusive,
                                  E toElement, boolean toInclusive)
    {
        return this;
    }

    @Override
    public NavigableSet<E> headSet(E toElement) {
        return this;
    }

    @Override
    public NavigableSet<E> headSet(E toElement, boolean inclusive) {
        return this;
    }

    @Override
    public NavigableSet<E> tailSet(E fromElement) {
        return this;
    }

    @Override
    public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
        return this;
    }

    @Override
    public int hashCode() {
        return 1716011460;
    }
}
