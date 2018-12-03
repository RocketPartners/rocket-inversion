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
package io.rcktapp.api.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atteo.evo.inflector.English;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.forty11.j.J;
import io.forty11.web.Url;
import io.forty11.web.js.JSObject;
import io.rcktapp.api.Action;
import io.rcktapp.api.Api;
import io.rcktapp.api.ApiException;
import io.rcktapp.api.Chain;
import io.rcktapp.api.Db;
import io.rcktapp.api.Endpoint;
import io.rcktapp.api.Request;
import io.rcktapp.api.Response;
import io.rcktapp.api.SC;
import io.rcktapp.api.handler.sql.SqlDb.ConnectionLocal;

public class Service extends HttpServlet
{
   Logger           log            = LoggerFactory.getLogger(getClass());
   Logger           requestLog     = LoggerFactory.getLogger(getClass() + ".requests");

   List<Api>        apis           = new Vector();

   /**
    * Must be set to match your servlet path if your servlet is not 
    * mapped to /*
    */
   protected String servletMapping = null;

   /**
    * Service reflects all request headers along with those supplied in <code>allowHeaders</code> as 
    * "Access-Control-Allow-Headers" response headers.  This is primarily a CROS security thing and you
    * probably won't need to customize this list. 
    */
   String           allowedHeaders = "accept,accept-encoding,accept-language,access-control-request-headers,access-control-request-method,authorization,connection,Content-Type,host,user-agent,x-auth-token";

   @Override
   protected void service(HttpServletRequest httpReq, HttpServletResponse httpResp) throws ServletException, IOException
   {
      String method = httpReq.getMethod();

      //--
      //-- CORS header setup
      //--
      String allowedHeaders = new String(this.allowedHeaders);
      String corsRequestHeader = httpReq.getHeader("Access-Control-Request-Header");
      if (corsRequestHeader != null)
      {
         List<String> headers = Arrays.asList(corsRequestHeader.split(","));
         for (String h : headers)
         {
            h = h.trim();
            allowedHeaders = allowedHeaders.concat(h).concat(",");
         }
      }
      httpResp.addHeader("Access-Control-Allow-Origin", "*");
      httpResp.addHeader("Access-Control-Allow-Credentials", "true");
      httpResp.addHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE");
      httpResp.addHeader("Access-Control-Allow-Headers", allowedHeaders);

      //--
      //-- End CORS Header Setup

      if (method.equalsIgnoreCase("options"))
      {
         //this is a CORS preflight request. All of hte work was done bove
         httpResp.setStatus(200);
         return;
      }

      if (httpReq.getRequestURI().indexOf("/favicon.ico") >= 0)
      {
         httpResp.setStatus(HttpServletResponse.SC_NOT_FOUND);
         return;
      }

      Api api = null;
      Response res = null;
      Request req = null;

      try
      {
         res = new Response(httpResp);

         String urlstr = httpReq.getRequestURL().toString();

         if (!urlstr.endsWith("/"))
            urlstr = urlstr + "/";

         String query = httpReq.getQueryString();
         if (!J.empty(query))
         {
            urlstr += "?" + query;
         }

         Url url = new Url(urlstr);

         String xfp = httpReq.getHeader("X-Forwarded-Proto");
         String xfh = httpReq.getHeader("X-Forwarded-Host");
         if (xfp != null || xfh != null)
         {
            if (xfp != null)
               url.setProtocol(xfp);

            if (xfh != null)
               url.setHost(xfh);
         }

         String apiUrl = null;
         ApiMatch match = findApi(method, url);
         if (match != null)
         {
            api = match.api;
            apiUrl = match.apiUrl;
         }

         if (match == null || match.api.isDebug() || match.reqUrl.getHost().equals("localhost"))
         {
            res.debug("");
            res.debug("");
            res.debug(">> request --------------");
            res.debug(method + ": " + url);
            Enumeration<String> e = httpReq.getHeaderNames();
            while (e.hasMoreElements())
            {
               String name = e.nextElement();
               String value = httpReq.getHeader(name);
               res.debug(name + " - " + value);
            }
         }

         if (match == null)
         {
            throw new ApiException(SC.SC_400_BAD_REQUEST, "No API found matching URL: \"" + url + "\"");
         }

         req = new Request(httpReq, match);

         if (match.endpoint == null)
         {
            //check to see if a non plural version of the collection endpoint 
            //was passed in, if it was redirect to the plural version
            if (redirectPlural(req, res))
               return;
         }

         if (match.endpoint == null)
         {
            String buff = "";
            for (Endpoint e : api.getEndpoints())
               buff += e.getMethods() + ": includePaths:" + e.getIncludePaths() + ": excludePaths" + e.getExcludePaths() + ",  ";

            throw new ApiException(SC.SC_404_NOT_FOUND, "No endpoint found matching \"" + req.getMethod() + ": " + url + "\" Valid end points include: " + buff);
         }

         if (J.empty(req.getCollectionKey()))
         {
            throw new ApiException(SC.SC_400_BAD_REQUEST, "It looks like your collectionKey is empty.  You need at least one more part to your url request path.");
         }

         doService(this, null, match, req, res);
         ConnectionLocal.commit();
      }
      catch (Throwable ex)
      {
         try
         {
            ConnectionLocal.rollback();
         }
         catch (Throwable t)
         {
            log.warn("Error rollowing back transaction", t);
         }

         String status = SC.SC_500_INTERNAL_SERVER_ERROR;

         if (ex instanceof ApiException)
         {
            if (req != null && req.isDebug())
            {
               log.error("Error in Service", ex);
            }

            status = ((ApiException) ex).getStatus();
            if (SC.SC_404_NOT_FOUND.equals(status))
            {
               //an endpoint could have match the url "such as GET * but then not 
               //known what to do with the URL because the collection was not pluralized
               if (redirectPlural(req, res))
                  return;
            }
         }
         else
         {
            log.error("Error in Service", ex);
         }

         res.setStatus(status);
         JSObject response = new JSObject("message", ex.getMessage());
         if (SC.SC_500_INTERNAL_SERVER_ERROR.equals(status))
            response.put("error", J.getShortCause(ex));

         res.setJson(response);
      }
      finally
      {
         try
         {
            ConnectionLocal.close();
         }
         catch (Throwable t)
         {
            log.warn("Error closing connections", t);
         }

         try
         {
            writeResponse(req, res);
         }
         catch (Throwable ex)
         {
            log.error("Error in Service", ex);
         }
      }
   }

   void writeResponse(Request req, Response res) throws Exception
   {
      boolean debug = req != null && req.isDebug();
      boolean explain = req != null && req.isExplain();

      String method = req != null ? req.getMethod() : null;

      HttpServletResponse http = res.getHttpResp();

      http.setStatus(res.getStatusCode());

      res.debug("\r\n<< response -------------\r\n");
      res.debug(res.getStatusCode());

      OutputStream out = http.getOutputStream();
      try
      {
         for (String key : res.getHeaders().keySet())
         {
            List values = res.getHeaders().get(key);
            StringBuffer buff = new StringBuffer();
            for (int i = 0; i < values.size(); i++)
            {
               buff.append(values.get(i));
               if (i < values.size() - 1)
                  buff.append(",");
            }
            http.setHeader(key, buff.toString());
            res.debug(key + " " + buff);
         } ;
         if ("OPTIONS".equals(method))
         {
            //
         }
         else if (res.getText() != null)
         {
            byte[] bytes = res.getText().getBytes();
            http.setContentType("text/text");
            out.write(bytes);
         }
         else if (res.getJson() != null)
         {
            byte[] bytes = res.getJson().toString().getBytes();

            res.addHeader("Content-Length", bytes.length + "");
            res.debug("Content-Length " + bytes.length + "");
            //http.setContentType("application/json; charset=utf-8");
            //http.setCharacterEncoding("UTF-8");
            http.setContentType("application/json");

            out(req, res, out, bytes);
         }

         res.debug("\r\n-- done -----------------\r\n");

         if (debug)
         {
            requestLog.info(res.getDebug());
         }

         if (explain)
         {
            out.write(res.getDebug().getBytes());
         }

      }
      finally
      {
         out.flush();
         out.close();
      }
   }

   void out(Request req, Response res, OutputStream out, byte[] bytes) throws Exception
   {
      res.debug("\r\n");
      res.debug(bytes);

      if (req == null || !req.isExplain())
         out.write(bytes);
   }

   /**
    * This method is designed to be called by handlers who want to "go back through the front door"
    * for additional functionality.
    */
   public Response include(Chain parent, String method, String url, String body) throws Exception
   {
      Api api = null;
      String apiUrl = null;
      ApiMatch match = findApi(method, new Url(url));
      if (match != null)
      {
         api = match.api;
         apiUrl = match.apiUrl;
      }

      if (match == null)
      {
         throw new ApiException(SC.SC_500_INTERNAL_SERVER_ERROR, "No api found matching " + method + " " + url);
      }

      Request req = new Request(null, match);//method, new Url(url), api, match, apiUrl);
      req.setUser(parent.getRequest().getUser());
      req.setBody(body);

      Response res = new Response(null);
      Endpoint endpoint = findEndpoint(api, req.getMethod(), req.getPath());

      if (endpoint == null)
      {
         throw new ApiException(SC.SC_500_INTERNAL_SERVER_ERROR, "No endpoint found matching " + method + " " + url);
      }

      try
      {
         doService(this, parent, match, req, res);
      }
      catch (Throwable ex)
      {
         String status = SC.SC_500_INTERNAL_SERVER_ERROR;

         if (ex instanceof ApiException)
         {
            log.error("Error in Service", ex);
            status = ((ApiException) ex).getStatus();
         }
         else
         {
            log.error("Error in Service", ex);
         }

         res.setStatus(status);
         JSObject response = new JSObject("message", ex.getMessage());
         if (SC.SC_500_INTERNAL_SERVER_ERROR.equals(status))
            response.put("error", J.getShortCause(ex));

         res.setJson(response);
      }
      finally
      {
         parent.getResponse().addChanges(res.getChanges());
      }

      return res;
   }

   protected void doService(Service service, Chain parent, ApiMatch match, Request req, Response res) throws Exception
   {
      //this will get all actions specifically configured on the endpoint
      List<Action> actions = match.endpoint.getActions(req);

      //this matches for actions that can run across multiple endpoints.
      //this might be something like an authorization or logging action
      //that acts like a filter
      for (Action a : match.api.getActions())
      {
         if (a.matches(req.getMethod(), req.getPath()))
            actions.add(a);
      }
      Collections.sort(actions);

      //TODO: filter all request params for security -- "restrict" && "require"

      Chain chain = new Chain(this, match.api, match.endpoint, actions, req, res);
      chain.setParent(parent);
      chain.go();
   }

   boolean redirectPlural(Request req, Response res) throws IOException
   {
      String collection = req.getCollectionKey();
      if (!J.empty(collection))
      {
         String plural = English.plural(collection);
         if (!plural.equals(collection))
         {
            String path = req.getPath();
            path = path.replaceFirst(collection, plural);
            Endpoint rightEndpoint = findEndpoint(req.getApi(), req.getMethod(), path);
            if (rightEndpoint != null)
            {
               String redirect = req.getHttpServletRequest().getRequestURI();
               //redirect = req.getHttpServletRequest().getRequest
               redirect = redirect.replaceFirst("\\/" + collection, "\\/" + plural);

               String queryString = req.getHttpServletRequest().getQueryString();
               if (!J.empty(queryString))
               {
                  redirect += "?" + queryString;
               }

               res.getHttpResp().sendRedirect(redirect);
               return true;
            }
         }
      }
      return false;
   }

   public static class ApiMatch
   {
      public Api      api      = null;
      public Endpoint endpoint = null;
      public String   method   = null;
      public Url      reqUrl   = null;
      public String   apiUrl   = null;
      public String   apiPath  = null;

      public ApiMatch(Api api, Endpoint endpoint, String method, Url reqUrl, String apiUrl, String apiPath)
      {
         this.api = api;
         this.endpoint = endpoint;
         this.method = method;
         this.reqUrl = reqUrl;
         this.apiUrl = apiUrl;
         this.apiPath = apiPath;
      }
   }

   ApiMatch findApi(String method, Url url) throws Exception
   {
      String accountCode = null;

      String path = url.getPath() + "";

      String host = url.getHost();
      if (host.indexOf(".") != host.lastIndexOf("."))//if this is a three part host name hostKey.domain.com
      {
         accountCode = host.substring(0, host.indexOf("."));
      }

      for (Api a : apis)
      {
         String fullPath = "/" + a.getAccountCode() + "/" + a.getApiCode() + "/";
         String halfPath = "/" + a.getApiCode() + "/";

         if (!J.empty(servletMapping))
         {
            fullPath = "/" + servletMapping + fullPath;
            halfPath = "/" + servletMapping + halfPath;
         }

         if ((accountCode == null && path.startsWith(fullPath)) || //  form: https://host.com/[${servletPath}]/${accountCode}/${apiCode}/
               (accountCode != null && accountCode.equals(a.getAccountCode()) && path.startsWith(fullPath)) || //form: https://host.com/[${servletPath}]/${accountCode}/${apiCode}/
               (accountCode != null && accountCode.equals(a.getAccountCode()) && path.startsWith(halfPath)) || //https://${accountCode}.host.com/[${servletPath}]/${apiCode}/
               (a.getAccountCode().equalsIgnoreCase(a.getApiCode()) && path.startsWith(halfPath))) //http/host.com/[${servletPath}]/${accountCode} ONLY when apiCode and accountCode are the same thing
         {

            if (path.startsWith(fullPath))
            {
               path = fullPath;
            }
            else
            {
               path = halfPath;
            }

            String apiUrl = url.toString();
            int idx = apiUrl.indexOf(path);
            apiUrl = apiUrl.substring(0, idx + path.length());

            if (a.isMultiTenant())
            {
               String u = url.toString();
               int start = apiUrl.length();
               int end = u.indexOf('/', start + 1);
               if (end < 0)
                  end = u.length();
               String tenantId = u.substring(start, end);

               if (!apiUrl.endsWith("/"))
                  apiUrl += "/";
               apiUrl += tenantId;
            }

            if (!apiUrl.endsWith("/"))
               apiUrl += "/";

            String reqUrl = url.toString();

            path = reqUrl.substring(apiUrl.length(), reqUrl.length());
            while (path.startsWith("/"))
               path = path.substring(1, path.length());

            if (!path.endsWith("/"))
               path = path + "/";

            Endpoint endpoint = null;
            for (Endpoint e : a.getEndpoints())
            {
               if (e.matches(method, path))
               {
                  endpoint = e;
                  break;
               }
            }

            return new ApiMatch(a, endpoint, method, url, apiUrl, path);
         }
      }

      return null;

   }

   Endpoint findEndpoint(Api api, String method, String path)
   {
      for (Endpoint endpoint : api.getEndpoints())
      {
         if (endpoint.matches(method, path))
            return endpoint;
      }
      return null;
   }

   public Db getDb(Api api, String collectionKey) throws ApiException
   {
      Db db = null;

      if (collectionKey != null)
      {
         db = api.findDb(collectionKey);
      }

      if (db == null)
      {
         if (api.getDbs() == null || api.getDbs().size() == 0)
            throw new ApiException(SC.SC_500_INTERNAL_SERVER_ERROR, "There are no database connections configured for this API.");
         db = api.getDbs().get(0);
      }

      return db;
   }

   public List<Api> getApis()
   {
      return new ArrayList(apis);
   }

   public synchronized void addApi(Api api)
   {
      List<Api> newList = new ArrayList(apis);

      Api existingApi = getApi(api.getAccountCode(), api.getApiCode());
      if (existingApi != null && existingApi != api)
      {
         newList.remove(existingApi);
         newList.add(api);
      }
      else if (existingApi == null)
      {
         newList.add(api);
      }

      if (existingApi != api)
         api.startup();

      apis = newList;

      if (existingApi != null && existingApi != api)
      {
         existingApi.shutdown();
      }
   }

   public synchronized void removeApi(Api api)
   {
      List newList = new ArrayList(apis);
      newList.remove(api);
      apis = newList;

      api.shutdown();
   }

   public synchronized Api getApi(String accountCode, String apiCode)
   {
      for (Api api : apis)
      {
         if (accountCode.equalsIgnoreCase(api.getAccountCode()) && apiCode.equalsIgnoreCase(api.getApiCode()))
            return api;
      }
      return null;
   }

   public static String buildLink(Request req, String collectionKey, Object entityKey, String subCollectionKey)
   {
      String url = req.getApiUrl();

      if (!J.empty(collectionKey))
      {
         if (!url.endsWith("/"))
            url += "/";

         url += collectionKey;
      }

      if (!J.empty(entityKey))
         url += "/" + entityKey;

      if (!J.empty(subCollectionKey))
         url += "/" + subCollectionKey;

      if (req.getApi().getUrl() != null && !url.startsWith(req.getApi().getUrl()))
      {
         String newUrl = req.getApi().getUrl();
         while (newUrl.endsWith("/"))
            newUrl = newUrl.substring(0, newUrl.length() - 1);

         url = newUrl + url.substring(url.indexOf("/", 8));
      }
      else
      {
         String proto = req.getHeader("x-forwarded-proto");
         if (!J.empty(proto))
         {
            url = proto + url.substring(url.indexOf(':'), url.length());
         }
      }

      return url;

   }

   public String getServletMapping()
   {
      return servletMapping;
   }

   public void setServletMapping(String servletMapping)
   {
      this.servletMapping = servletMapping;
   }

   public void setAllowHeaders(String allowedHeaders)
   {
      this.allowedHeaders = allowedHeaders;
   }

   public InputStream getResource(String name)
   {
      return getServletContext().getResourceAsStream(name);
   }
}
