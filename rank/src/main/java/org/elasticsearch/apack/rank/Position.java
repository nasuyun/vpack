package org.elasticsearch.apack.rank;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public  class Position implements Writeable {

    public final String id;
    public final int pos;

    public Position(String id, int pos) {
        this.id = id;
        this.pos = pos;
    }

    public Position(StreamInput in) throws IOException {
        Map.Entry<String, Object> entry = in.readMap().entrySet().iterator().next();
        id = entry.getKey();
        pos = (Integer) entry.getValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put(id, pos);
        out.writeMap(map);
    }
}