/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

class GetTrialStatusRequestBuilder extends ActionRequestBuilder<GetTrialStatusRequest, GetTrialStatusResponse> {

    GetTrialStatusRequestBuilder(ElasticsearchClient client, GetTrialStatusAction action) {
        super(client, action, new GetTrialStatusRequest());
    }
}
