/*
 * Copyright 2024 Tabular Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.rest;

import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.util.PropertyUtil;

import java.util.Map;

public class RESTServerCatalogAdapter extends RESTCatalogAdapter {
    private static final String INCLUDE_CREDENTIALS = "include-credentials";

    private final RESTCatalogServer.CatalogContext catalogContext;


    public RESTServerCatalogAdapter(RESTCatalogServer.CatalogContext catalogContext) {
        super(catalogContext.catalog());

        this.catalogContext = catalogContext;
    }

    @Override
    public <T extends RESTResponse> T handleRequest(Route route, Map<String, String> vars, Object body, Class<T> responseType) {
        T restResponse = super.handleRequest(route, vars, body, responseType);

        if (restResponse instanceof LoadTableResponse loadTableResponse) {
            if (PropertyUtil.propertyAsBoolean(catalogContext.configuration(), INCLUDE_CREDENTIALS, false)) {
                applyCredentials(catalogContext.configuration(), loadTableResponse.config());
            }
        }

        return restResponse;
    }

    private void applyCredentials(Map<String, String> catalogConfig, Map<String, String> tableConfig) {

        if (catalogConfig.containsKey(GCPProperties.GCS_OAUTH2_TOKEN)) {
            tableConfig.put(GCPProperties.GCS_OAUTH2_TOKEN, catalogConfig.get(GCPProperties.GCS_OAUTH2_TOKEN));
        }
    }
}
