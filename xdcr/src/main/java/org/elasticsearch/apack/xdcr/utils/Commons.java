package org.elasticsearch.apack.xdcr.utils;

import java.util.concurrent.TimeUnit;

public class Commons {

    public static void sleep(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
        }
    }
}
