/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.plain.schema;

import org.elasticsearch.Version;
import org.elasticsearch.apack.plain.PlainIllegalArgumentException;
import org.elasticsearch.common.io.stream.*;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Registry and utilities around {@link Cursor}s.
 */
public final class Cursors {

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(getNamedWriteables());

    private Cursors() {
    }

    /**
     * The {@link NamedWriteable}s required to deserialize {@link Cursor}s.
     */
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        // cursors
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, EmptyCursor.NAME, in -> Cursor.EMPTY));
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, TextFormatterCursor.NAME, TextFormatterCursor::new));
        return entries;
    }

    /**
     * Write a {@linkplain Cursor} to a string for serialization across xcontent.
     */
    public static String encodeToString(Version version, Cursor info) {
        if (info == Cursor.EMPTY) {
            return "";
        }
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            try (OutputStream base64 = Base64.getEncoder().wrap(os); StreamOutput out = new OutputStreamStreamOutput(base64)) {
                Version.writeVersion(version, out);
                out.writeNamedWriteable(info);
            }
            return os.toString(StandardCharsets.UTF_8.name());
        } catch (Exception ex) {
            throw new PlainIllegalArgumentException("Unexpected failure retrieving next page", ex);
        }
    }


    /**
     * Read a {@linkplain Cursor} from a string.
     */
    public static Cursor decodeFromString(String info) {
        if (info.isEmpty()) {
            return Cursor.EMPTY;
        }
        byte[] bytes = info.getBytes(StandardCharsets.UTF_8);
        try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(Base64.getDecoder().decode(bytes)), WRITEABLE_REGISTRY)) {
            Version version = Version.readVersion(in);
            if (version.after(Version.CURRENT)) {
                throw new PlainIllegalArgumentException("Unsupported cursor version " + version);
            }
            in.setVersion(version);
            return in.readNamedWriteable(Cursor.class);
        } catch (PlainIllegalArgumentException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new PlainIllegalArgumentException("Unexpected failure decoding cursor", ex);
        }
    }
}