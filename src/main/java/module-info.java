
module org.cojen.tupl {
    exports org.cojen.tupl;
    exports org.cojen.tupl.ext;
    exports org.cojen.tupl.io;
    exports org.cojen.tupl.repl;
    exports org.cojen.tupl.tools;
    exports org.cojen.tupl.util;

    requires transitive java.logging;
    requires transitive java.management;

    requires transitive jdk.unsupported;

    requires transitive com.sun.jna;
    requires transitive com.sun.jna.platform;
}
