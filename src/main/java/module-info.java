
/**
 * See {@link org.cojen.tupl.Database} to get started with Tupl.
 */
module org.cojen.tupl {
    exports org.cojen.tupl;
    exports org.cojen.tupl.ext;
    exports org.cojen.tupl.io;
    exports org.cojen.tupl.repl;
    exports org.cojen.tupl.tools;
    exports org.cojen.tupl.util;

    requires transitive java.logging;

    requires transitive jdk.unsupported;

    requires transitive com.sun.jna;
    requires transitive com.sun.jna.platform;

    requires static java.management;

    requires static org.lz4.java;

    requires static org.slf4j;
}
