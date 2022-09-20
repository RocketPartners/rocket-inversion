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
package io.rocketpartners.cloud.model;

import io.rocketpartners.cloud.service.Chain;
import io.rocketpartners.cloud.service.Service;
import io.rocketpartners.cloud.utils.Utils;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wells
 */
public abstract class Action<A extends Action> extends Rule<A> {
  protected String comment = null;

  public Action() {}

  public Action(String includePaths, String excludePaths, String config) {
    withConfig(config);
    withIncludePaths(includePaths);
    withExcludePaths(excludePaths);
  }

  public static List<ObjectNode> find(Object parent, String... paths) {
    List<ObjectNode> found = new ArrayList<>();
    for (String apath : paths) {
      for (String path : (List<String>) Utils.explode(",", apath)) {
        find(parent, found, path, ".");
      }
    }
    return found;
  }

  public static void find(
      Object parent, List<ObjectNode> found, String targetPath, String currentPath) {
    if (parent instanceof ArrayNode) {
      for (Object child : (ArrayNode) parent) {
        if (child instanceof ObjectNode) find(child, found, targetPath, currentPath);
      }
    } else if (parent instanceof ObjectNode) {
      if (!found.contains(parent) && Utils.wildcardMatch(targetPath, currentPath)) {
        found.add((ObjectNode) parent);
      }

      for (String key : ((ObjectNode) parent).keySet()) {
        Object child = ((ObjectNode) parent).get(key);
        String nextPath =
            currentPath == null || currentPath.length() == 0
                ? key
                : currentPath + key.toLowerCase() + ".";
        find(child, found, targetPath, nextPath);
      }
    }
  }

  public static String getValue(Chain chain, String key) {
    //      if ("apiId".equalsIgnoreCase(key))
    //      {
    //         return chain.getRequest().getApi().getId() + "";
    //      }
    //      else
    if ("apiCode".equalsIgnoreCase(key)) {
      return chain.getRequest().getApi().getApiCode();
    }
    //      else if ("accountId".equalsIgnoreCase(key))
    //      {
    //         return chain.getRequest().getApi().getAccountId() + "";
    //      }
    else if ("tenantId".equalsIgnoreCase(key)) {
      if (chain.getRequest().getUser() != null)
        return chain.getRequest().getUser().getTenantId() + "";
    } else if ("tenantCode".equalsIgnoreCase(key)) {
      if (chain.getRequest().getUser() != null) return chain.getRequest().getUser().getTenantCode();
    } else if ("userId".equalsIgnoreCase(key)) {
      if (chain.getRequest().getUser() != null) return chain.getRequest().getUser().getId() + "";
    } else if ("username".equalsIgnoreCase(key)) {
      if (chain.getRequest().getUser() != null) return chain.getRequest().getUser().getUsername();
    }

    Object val = chain.get(key);
    if (val != null) return val.toString();
    return null;
  }

  public static String nextPath(String path, String next) {
    return Utils.empty(path) ? next : path + "." + next;
  }

  public void run(
      Service service, Api api, Endpoint endpoint, Chain chain, Request req, Response res)
      throws Exception {}

  @Override
  public A withApi(Api api) {
    if (this.api != api) {
      this.api = api;
      // intentionally not bidirectional
      // api.withAction(this);
    }
    return (A) this;
  }

  public String getComment() {
    return comment;
  }

  public A withComment(String comment) {
    this.comment = comment;
    return (A) this;
  }
}
