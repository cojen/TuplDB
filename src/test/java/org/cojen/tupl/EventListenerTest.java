/*
 *  Copyright (C) 2017 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.logging.Level;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class EventListenerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(EventListenerTest.class.getName());
    }

    @Test
    public void observe() throws Exception {
        Listener listener = new Listener();
        listener.notify(EventType.DEBUG, "hello");
        assertEquals("[DEBUG:hello:[]]", listener.mEvents.toString());

        listener = new Listener();
        EventListener filtered = listener.observe(EventType.Category.DEBUG);
        filtered.notify(EventType.DEBUG, "hello");
        assertEquals("[DEBUG:hello:[]]", listener.mEvents.toString());
        assertTrue(filtered == filtered.observe(EventType.Category.DEBUG));

        listener = new Listener();
        filtered = listener.observe(new EventType.Category[0]);
        filtered.notify(EventType.DEBUG, "hello");
        assertEquals("[]", listener.mEvents.toString());
        assertTrue(filtered == filtered.observe(EventType.Category.DEBUG));

        listener = new Listener();
        filtered = listener.observe(EventType.Category.DEBUG, EventType.Category.DELETION);
        filtered.notify(EventType.DEBUG, "hello");
        filtered.notify(EventType.DELETION_FAILED, "world", 1);
        assertEquals("[DEBUG:hello:[], DELETION_FAILED:world:[1]]", listener.mEvents.toString());
        filtered = listener.observe(Level.WARNING);
        listener.mEvents.clear();
        filtered.notify(EventType.DEBUG, "hello");
        filtered.notify(EventType.DELETION_FAILED, "world", 2);
        assertEquals("[DELETION_FAILED:world:[2]]", listener.mEvents.toString());
        filtered = filtered.observe(Level.INFO);
        listener.mEvents.clear();
        filtered.notify(EventType.DEBUG, "hello");
        filtered.notify(EventType.DELETION_FAILED, "world", 3);
        assertEquals("[]", listener.mEvents.toString());
    }

    @Test
    public void ignore() throws Exception {
        Listener listener = new Listener();
        assertTrue(listener == listener.ignore(new EventType.Category[0]));
        assertTrue(listener == listener.ignore(new Level[0]));

        EventListener filtered = listener.ignore(EventType.Category.DEBUG);
        filtered.notify(EventType.DEBUG, "hello");
        filtered.notify(EventType.CHECKPOINT_BEGIN, "world");
        assertEquals("[CHECKPOINT_BEGIN:world:[]]", listener.mEvents.toString());
        assertTrue(filtered == filtered.ignore(EventType.Category.DEBUG));

        listener = new Listener();
        filtered = listener.ignore(Level.FINE);
        filtered.notify(EventType.DEBUG, "hello");
        filtered.notify(EventType.CHECKPOINT_FAILED, "world");
        assertEquals("[CHECKPOINT_FAILED:world:[]]", listener.mEvents.toString());
        assertTrue(filtered == filtered.ignore(Level.FINE));
        listener.mEvents.clear();
        filtered = filtered.ignore(Level.WARNING);
        filtered.notify(EventType.DEBUG, "hello");
        filtered.notify(EventType.CHECKPOINT_FAILED, "world");
        assertEquals("[]", listener.mEvents.toString());
        listener.mEvents.clear();
        filtered = filtered.observe(Level.WARNING);
        filtered.notify(EventType.DEBUG, "hello");
        filtered.notify(EventType.CHECKPOINT_FAILED, "world");
        assertEquals("[]", listener.mEvents.toString());
    }

    static class Listener implements EventListener {
        final List<String> mEvents = new ArrayList<>();

        @Override
        public void notify(EventType type, String message, Object... args) {
            mEvents.add(type.toString() + ":" + message + ":" + Arrays.toString(args));
        }
    }
}
