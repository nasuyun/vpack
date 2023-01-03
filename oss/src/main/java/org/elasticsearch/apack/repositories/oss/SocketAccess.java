package org.elasticsearch.apack.repositories.oss;

import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.SpecialPermission;

import java.io.IOException;
import java.net.SocketPermission;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public final class SocketAccess {

    private SocketAccess() {
    }

    public static <T> T doPrivilegedIOException(PrivilegedExceptionAction<T> operation) {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            Throwables.rethrow(e.getCause());
            assert false : "always throws";
            return null;
        }
    }

    public static <T> T doPrivilegedException(PrivilegedExceptionAction<T> operation) {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            Throwables.rethrow(e.getCause());
            assert false : "always throws";
            return null;
        }
    }

    public static void doPrivilegedVoidException(StorageRunnable action) {
        SpecialPermission.check();
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                action.executeCouldThrow();
                return null;
            });
        } catch (PrivilegedActionException e) {
            Throwables.rethrow(e.getCause());
        }
    }

    @FunctionalInterface
    public interface StorageRunnable {
        void executeCouldThrow() throws URISyntaxException, IOException;
    }

}
