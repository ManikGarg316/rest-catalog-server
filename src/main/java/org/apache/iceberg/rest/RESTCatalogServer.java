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

import jakarta.servlet.Servlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.util.PropertyUtil;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class RESTCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogServer.class);
  private static final String CATALOG_ENV_PREFIX = "CATALOG_";

  private RESTCatalogServer() {}

  record CatalogContext(Catalog catalog, Map<String,String> configuration) { }

  private static CatalogContext backendCatalog1() throws IOException {
    // Translate environment variable to catalog properties
    System.setProperty("hadoop.home.dir", "/Users/navi/Desktop/Digipass/iceberg-rest-catalog-server");
    Map<String, String> catalogProperties =
        System.getenv().entrySet().stream()
            .filter(e -> e.getKey().startsWith(CATALOG_ENV_PREFIX))
            .collect(
                Collectors.toMap(
                    e ->
                        e.getKey()
                            .replaceFirst(CATALOG_ENV_PREFIX, "")
                            .replaceAll("__", "-")
                            .replaceAll("_", ".")
                            .toLowerCase(Locale.ROOT),
                    Map.Entry::getValue,
                    (m1, m2) -> {
                      throw new IllegalArgumentException("Duplicate key: " + m1);
                    },
                    HashMap::new));
    catalogProperties.put("catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog");
    // Fallback to a JDBCCatalog impl if one is not set
    catalogProperties.putIfAbsent(
        CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.jdbc.JdbcCatalog");
    catalogProperties.put("warehouse", "gs://iceberg_data_bucket/warehouse");
    // Configure a default location if one is not specified
    String warehouseLocation = catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION);

    if (warehouseLocation == null) {
      File tmp = java.nio.file.Files.createTempDirectory("iceberg_warehouse").toFile();
      tmp.deleteOnExit();
      warehouseLocation = tmp.toPath().resolve("iceberg_data").toFile().getAbsolutePath();
      catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

      LOG.info("No warehouse location set.  Defaulting to temp location: {}", warehouseLocation);
    }

    LOG.info("Creating catalog with properties: {}", catalogProperties);
    return new CatalogContext(CatalogUtil.buildIcebergCatalog("rest_backend", catalogProperties, new Configuration()), catalogProperties);
  }

  private static CatalogContext backendCatalog2() throws IOException {
    // Translate environment variable to catalog properties
    Map<String, String> catalogProperties =
            System.getenv().entrySet().stream()
                    .filter(e -> e.getKey().startsWith(CATALOG_ENV_PREFIX))
                    .collect(
                            Collectors.toMap(
                                    e ->
                                            e.getKey()
                                                    .replaceFirst(CATALOG_ENV_PREFIX, "")
                                                    .replaceAll("__", "-")
                                                    .replaceAll("_", ".")
                                                    .toLowerCase(Locale.ROOT),
                                    Map.Entry::getValue,
                                    (m1, m2) -> {
                                      throw new IllegalArgumentException("Duplicate key: " + m1);
                                    },
                                    HashMap::new));
    catalogProperties.put("catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog");
    // Fallback to a JDBCCatalog impl if one is not set
    catalogProperties.putIfAbsent(
            CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.jdbc.JdbcCatalog");
    catalogProperties.put("warehouse", "gs://catalog_bucket_test/warehouse");
    // Configure a default location if one is not specified
    String warehouseLocation = catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION);

    if (warehouseLocation == null) {
      File tmp = java.nio.file.Files.createTempDirectory("iceberg_warehouse").toFile();
      tmp.deleteOnExit();
      warehouseLocation = tmp.toPath().resolve("iceberg_data").toFile().getAbsolutePath();
      catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

      LOG.info("No warehouse location set.  Defaulting to temp location: {}", warehouseLocation);
    }

    LOG.info("Creating catalog with properties: {}", catalogProperties);
    return new CatalogContext(CatalogUtil.buildIcebergCatalog("rest_backend", catalogProperties, new Configuration()), catalogProperties);
  }

  public static void main(String[] args) throws Exception {
    CatalogContext catalogContext1 = backendCatalog1();
    ServletContextHandler context1 = getServletContextHandler(catalogContext1, "/catalog1");

    CatalogContext catalogContext2 = backendCatalog2();
    ServletContextHandler context2 = getServletContextHandler(catalogContext2, "/catalog2");

    HandlerCollection handlers = new HandlerList();
    handlers.addHandler(context1);
    handlers.addHandler(context2);

    Server httpServer =
          new Server(PropertyUtil.propertyAsInt(System.getenv(), "REST_PORT", 8181));
      httpServer.setHandler(handlers);
      httpServer.start();
      httpServer.join();
    }

  private static ServletContextHandler getServletContextHandler(CatalogContext catalogContext, String contextPath) {
    RESTCatalogAdapter adapter = new RESTServerCatalogAdapter(catalogContext);
    IcebergRestCatalogServlet servlet = new IcebergRestCatalogServlet(adapter);
    ServletContextHandler context1 = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context1.setContextPath(contextPath);
    ServletHolder servletHolder1 = new ServletHolder(servlet);
    servletHolder1.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
    context1.addServlet(servletHolder1, "/*");
    context1.setVirtualHosts(null);
    return context1;
  }
}
