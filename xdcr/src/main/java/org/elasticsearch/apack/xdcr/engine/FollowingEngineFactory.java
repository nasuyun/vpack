package org.elasticsearch.apack.xdcr.engine;

import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;

public final class FollowingEngineFactory implements EngineFactory {

    @Override
    public Engine newReadWriteEngine(final EngineConfig config) {
        return new FollowingEngine(config);
    }

}
