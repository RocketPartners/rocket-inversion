/*
 * Copyright (c) 2015-2018 Rocket Partners, LLC
 * http://rocketpartners.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */
package io.rcktapp.rql.elastic;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds the top-level Elasticsearch `rescore` block. The outer query (filter
 * context) does the matching and pays no scoring cost; rescore runs a `match`
 * query against only the top `windowSize` documents each shard returns, so
 * relevance ranking is applied without scaling with the total matched set.
 *
 * Emits:
 * {
 *   "window_size": N,
 *   "query": {
 *     "rescore_query": { "match": { "field": "queryText" } },
 *     "query_weight": 0,
 *     "rescore_query_weight": 1
 *   }
 * }
 */
public class Rescore
{
   public static final int DEFAULT_WINDOW_SIZE = 200;

   private final String field;
   private final String queryText;
   private final int    windowSize;

   public Rescore(String field, String queryText, int windowSize)
   {
      this.field = field;
      this.queryText = queryText;
      this.windowSize = windowSize > 0 ? windowSize : DEFAULT_WINDOW_SIZE;
   }

   public Map<String, Object> toMap()
   {
      Map<String, Object> rescoreQuery = new LinkedHashMap<String, Object>();
      rescoreQuery.put("rescore_query", Collections.singletonMap("match", Collections.singletonMap(field, queryText)));
      rescoreQuery.put("query_weight", 0);
      rescoreQuery.put("rescore_query_weight", 1);

      Map<String, Object> rescore = new LinkedHashMap<String, Object>();
      rescore.put("window_size", windowSize);
      rescore.put("query", rescoreQuery);
      return rescore;
   }

   public String getField()
   {
      return field;
   }

   public String getQueryText()
   {
      return queryText;
   }

   public int getWindowSize()
   {
      return windowSize;
   }
}
