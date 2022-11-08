/**
 * Copyright (c) 2016, SIREn Solutions. All Rights Reserved.
 * <p>
 * This file is part of the SIREn project.
 * <p>
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * <p>
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, .
 */
package org.elasticsearch.apack.terms;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequestOptions;

/**
 * The action to request terms by query
 */
public class TermsFetchAction extends Action<TermsFetchRequest, TermsFetchResponse, TermsFetchRequestBuilder> {

    public static final TermsFetchAction INSTANCE = new TermsFetchAction();
    public static final String NAME = "indices:data/read/field/terms";

    /**
     * Default constructor
     */
    private TermsFetchAction() {
        super(NAME);
    }

    /**
     * Gets a new {@link TermsFetchResponse} object
     *
     * @return the new {@link TermsFetchResponse}.
     */
    @Override
    public TermsFetchResponse newResponse() {
        return new TermsFetchResponse();
    }

    /**
     * Set transport options specific to a terms by query request.
     * Enabling compression here does not really reduce data transfer, even increase it on the contrary.
     *
     * @param settings node settings
     * @return the request options.
     */
    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.builder()
                .withType(TransportRequestOptions.Type.REG)
                .build();
    }

    /**
     * Get a new {@link TermsFetchRequestBuilder}
     *
     * @param client the client responsible for executing the request.
     * @return the new {@link TermsFetchRequestBuilder}
     */
    @Override
    public TermsFetchRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new TermsFetchRequestBuilder(client, this);
    }

}
