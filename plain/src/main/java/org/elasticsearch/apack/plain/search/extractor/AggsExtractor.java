package org.elasticsearch.apack.plain.search.extractor;

import org.elasticsearch.common.io.stream.NamedWriteable;

public interface AggsExtractor<T> extends NamedWriteable {

    Object extract(T response);

}
