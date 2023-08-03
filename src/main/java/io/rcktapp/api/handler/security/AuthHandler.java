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
package io.rcktapp.api.handler.security;

import io.forty11.j.J;
import io.forty11.sql.Sql;
import io.forty11.web.js.JSArray;
import io.forty11.web.js.JSObject;
import io.rcktapp.api.Action;
import io.rcktapp.api.Api;
import io.rcktapp.api.ApiException;
import io.rcktapp.api.Chain;
import io.rcktapp.api.Endpoint;
import io.rcktapp.api.Handler;
import io.rcktapp.api.Permission;
import io.rcktapp.api.Request;
import io.rcktapp.api.Response;
import io.rcktapp.api.Role;
import io.rcktapp.api.SC;
import io.rcktapp.api.User;
import io.rcktapp.api.handler.sql.SqlDb;
import io.rcktapp.api.service.Service;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections.map.LRUMap;

import java.security.MessageDigest;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class AuthHandler implements Handler
{
   long                       sessionExp              = 1000 * 60 * 30; //30 minute default timeout
   long                       sessionUpdate           = 1000 * 20;      //update a session every 10s to prevent spamming the cache with every request
   protected int              sessionMax              = 10000;

   protected int              failedMax               = 10;
   protected int              failedExp               = 1000 * 60 * 10; //10 minute timeout for failed password attemtps

   protected String           collection              = null;

   protected String           authenticatedPerm       = null;           // apply this perm to all authenticated users, allows ACL to target all authenticated users

   protected AuthSessionCache sessionCache            = null;

   protected SqlDb            db                      = null;

   protected boolean          shouldTrackRequestTimes = true;

   @Override
   public void service(Service service, Api api, Endpoint endpoint, Action action, Chain chain, Request req, Response resp) throws Exception
   {
      //one time init
      if (sessionCache == null)
      {
         synchronized (this)
         {
            if (sessionCache == null)
               sessionCache = new LRUAuthSessionCache(sessionMax);
         }
      }

      User user = req.getUser();

      if (user != null && !req.isDelete())
      {
         //the users is already logged in, have to let
         //deletes through because this could be a logout
         return;
      }

      String collection = action.getConfig("collection", this.collection);
      String authenticatedPerm = action.getConfig("authenticatedPerm", this.authenticatedPerm);
      long failedMax = Long.parseLong(action.getConfig("failedMax", this.failedMax + ""));
      long sessionExp = Long.parseLong(action.getConfig("sessionExp", this.sessionExp + ""));

      //-- END CONFIG

      long now = System.currentTimeMillis();

      String username = null;
      String password = null;
      String sessionKey = null;
      boolean sessionReq = collection != null && collection.equalsIgnoreCase(req.getCollectionKey());

      String url = req.getUrl().toString().toLowerCase();
      while (url.endsWith("/"))
         url = url.substring(0, url.length() - 1);

      String token = req.getHeader("authorization");
      if (token == null)
         token = req.getHeader("x-auth-token");

      if (token != null)
      {
         token = token.trim().toLowerCase();
         if (token.startsWith("session "))
         {
            sessionKey = token.toLowerCase().substring(8, token.length()).trim();
         }
         else if (token.startsWith("basic "))
         {
            token = token.substring(token.indexOf(" ") + 1, token.length());
            token = new String(Base64.decodeBase64(token));
            username = token.substring(0, token.indexOf(":"));
            password = token.substring(token.indexOf(":") + 1, token.length());
         }
      }

      if (req.isPost() && sessionReq && (J.empty(username, password)))
      {
         username = req.getJson().getString("username");
         password = req.getJson().getString("password");
      }

      if (sessionKey == null && J.empty(username, password))
      {
         username = req.removeParam("x-auth-username");
         password = req.removeParam("x-auth-password");
      }

      if (sessionKey == null && J.empty(username, password))
      {
         username = req.getHeader("username");
         password = req.getHeader("password");
      }

      if (sessionKey == null && J.empty(username, password))
      {
         username = req.removeParam("username");
         password = req.removeParam("password");
      }

      if (sessionReq && req.isDelete())
      {
         //this is a logout

         //delete to http[s]://{host}/{collection}/{sessionKey}
         if (sessionKey == null)
            sessionKey = url.substring(url.lastIndexOf("/") + 1, url.length());

         if (sessionKey == null)
            throw new ApiException(SC.SC_400_BAD_REQUEST, "Logout requires a session authorization or x-auth-token header");

         sessionCache.remove(sessionKey);
      }
      else if (sessionReq && req.isGet())
      {
         // This will return status info about the session
         // useful for client applications checking to see if the user is still logged in
         if (sessionKey == null)
         {
            throw new ApiException(SC.SC_400_BAD_REQUEST, "GET session requires a session authorization or x-auth-token header");
         }

         user = sessionCache.get(sessionKey);

         if (user == null)
         {
            throw new ApiException(SC.SC_401_UNAUTHORIZED);
         }

         resp.setJson(buildSessionJson(user, now));

         chain.cancel();
         return;

      }
      else if (!J.empty(username, password))
      {
         Connection conn = db.getConnection();

         User tempUser = getUser(conn, api, req.getTenantCode(), username, null);
         boolean authorized = false;
         if (tempUser != null)
         {
            long requestAt = tempUser.getRequestAt();
            int failedNum = tempUser.getFailedNum();
            if (failedNum < failedMax || now - requestAt > failedExp)
            {
               //only attempt to validate password and log the attempt 
               //if the user has failed login fewer than failedMax times
               String remoteAddr = req.getRemoteAddr();
               authorized = checkPassword(conn, tempUser, password);

               if (shouldTrackRequestTimes)
               {
                  String sql = "UPDATE User SET requestAt = ?, failedNum = ?, remoteAddr = ? WHERE id = ?";
                  Sql.execute(conn, sql, now, authorized ? 0 : failedNum + 1, remoteAddr, tempUser.getId());
               }

               if (authorized)
               {
                  tempUser.setRequestAt(now);
                  tempUser.setRoles(getRoles(conn, req.getApi(), tempUser));
                  tempUser.setPermissions(getPermissions(conn, req.getApi(), tempUser));
                  if (!J.empty(authenticatedPerm))
                  {
                     tempUser.getPermissions().add(new Permission(authenticatedPerm));
                  }

                  user = tempUser;
               }
            }
         }

         if (tempUser == null || !authorized)
            throw new ApiException(SC.SC_401_UNAUTHORIZED);

      }

      if (sessionKey != null)
      {
         user = sessionCache.get(sessionKey);
         if (user != null && sessionExp > 0)
         {
            if (now - user.getRequestAt() > sessionExp)
            {
               sessionCache.remove(sessionKey);
               user = null;
            }
         }

         if (user == null)
            throw new ApiException(SC.SC_401_UNAUTHORIZED);
      }

      if (user != null)
      {
         // update the session cache if this is a session request OR 
         // if the session update time has passed.
         boolean updateSessionCache = false;
         long previousReqTime = user.getRequestAt();
         if (now - previousReqTime > sessionUpdate)
            updateSessionCache = true;

         user.setRequestAt(now);
         req.setUser(user);

         if (sessionReq && req.isPost())
         {
            sessionKey = req.getApi().getId() + "_" + newSessionId();
            updateSessionCache = true;

            resp.addHeader("x-auth-token", "Session " + sessionKey);
            resp.setJson(buildSessionJson(user, now));
         }

         if (updateSessionCache)
            sessionCache.put(sessionKey, user);
      }

      if (req.getUser() != null)
      {
         User loggedIn = req.getUser();
         if (api.isMultiTenant() && (req.getTenantCode() == null || !req.getTenantCode().equalsIgnoreCase(loggedIn.getTenantCode())))
            throw new ApiException(SC.SC_401_UNAUTHORIZED);
      }

      if (user == null && !sessionReq)
      {
         user = new User();
         user.setUsername("Anonymous");
         user.setRoles(Arrays.asList(new Role("guest")));

         if (api.isMultiTenant())
         {
            String tenantCode = req.getTenantCode();

            Integer tenantId = (Integer) api.getCache("TENANT_ID_" + tenantCode);
            if (tenantId == null)
            {
               Connection conn = db.getConnection();

               Object tenant = Sql.selectValue(conn, "SELECT id FROM Tenant WHERE tenantCode = ?", tenantCode);
               if (tenant == null)
                  throw new ApiException(SC.SC_404_NOT_FOUND);

               tenantId = Integer.parseInt(tenant + "");
               api.putCache("TENANT_ID_" + tenantCode, tenantId);
            }

            user.setTenantCode(tenantCode);
            user.setTenantId(tenantId);
         }
         req.setUser(user);
      }

   }

   protected JSObject buildSessionJson(User user, long now)
   {
      JSObject obj = new JSObject();
      obj.put("id", user.getId());
      obj.put("username", user.getUsername());
      obj.put("displayname", user.getDisplayName());
      obj.put("lastUpdate", user.getRequestAt());
      obj.put("expiresIn", user.getRequestAt() + sessionExp - now);

      JSArray perms = new JSArray();
      for (Permission perm : user.getPermissions())
      {
         perms.add(perm.getName());
      }
      obj.put("perms", perms);

      JSArray roles = new JSArray();
      for (Role role : user.getRoles())
      {
         roles.add(role.getName());
      }
      obj.put("roles", roles);

      return new JSObject("data", obj);
   }

   protected User getUser(Connection conn, Api api, String tenantCode, String username, String accessKey) throws Exception
   {
      if (J.empty(username, accessKey))
         throw new ApiException(SC.SC_401_UNAUTHORIZED);

      if (api.isMultiTenant() && J.empty(tenantCode))
         throw new ApiException(SC.SC_401_UNAUTHORIZED);

      String sql = "";
      List params = new ArrayList();
      if (api.isMultiTenant())
      {
         sql += " SELECT DISTINCT u.*, t.id AS tenantId, t.tenantCode ";
         sql += " FROM User u   ";
         sql += " JOIN Tenant t ON t.tenantCode = ? ";
         params.add(tenantCode);
      }
      else
      {
         sql += " SELECT DISTINCT u.*";
         sql += " FROM User u   ";
      }

      sql += " WHERE (u.revoked IS NULL OR u.revoked != 1) ";

      if (!J.empty(username))
      {
         sql += " AND u.username = ? ";
         params.add(username);
      }
      else
      {
         sql += " AND u.accessKey = ? ";
         params.add(accessKey);
      }
      sql += " LIMIT 1 ";

      return Sql.selectObject(conn, sql, User.class, params);
   }

   boolean checkPassword(Connection conn, User user, String password)
   {
      boolean matched = false;

      String savedHash = user.getPassword();
      String newHash = hashPassword(user.getId(), password);

      if (savedHash.equals(newHash))
      {
         matched = true;
      }
      else if (savedHash.equalsIgnoreCase(md5(password.getBytes())))
      {
         //this allows for manual password recovery by manually putting an
         //md5 of the password into the password col in the db
         matched = true;
      }

      return matched;
   }

   protected List<Role> getRoles(Connection conn, Api api, User user) throws Exception
   {
      String sql = "";
      sql += " SELECT DISTINCT r.* ";
      sql += " FROM Role r JOIN UserRole ur ON ur.roleId = r.id AND ur.userId = ? and ur.accountId = ?";
      return Sql.selectObjects(conn, sql, Role.class, user.getId(), api.getAccountId());
   }

   protected List<Permission> getPermissions(Connection conn, Api api, User user) throws Exception
   {
      String sql = "";
      sql += "\r\n SELECT DISTINCT * ";
      sql += "\r\n  FROM ";
      sql += "\r\n  ( ";
      sql += "\r\n    SELECT p.id, p.name ";
      sql += "\r\n    FROM Permission p";
      sql += "\r\n    JOIN UserPermission up ON p.id = up.permissionId";
      sql += "\r\n    WHERE up.userId = ? AND up.apiId = ? AND up.tenantId = ? ";
      sql += "\r\n                                                           ";
      sql += "\r\n    UNION";
      sql += "\r\n                                                           ";
      sql += "\r\n    SELECT p.id, p.name";
      sql += "\r\n    FROM Permission p";
      sql += "\r\n    JOIN GroupPermission gp ON p.id = gp.permissionId";
      sql += "\r\n    JOIN UserGroup ug ON ug.groupId = gp.groupId ";
      sql += "\r\n    WHERE ug.userId = ? and gp.apiId = ? AND gp.tenantId = ? ";
      sql += "\r\n  ) as perms";

      List args = Arrays.asList(user.getId(), api.getId(), user.getTenantId(), user.getId(), api.getId(), user.getTenantId());
      return Sql.selectObjects(conn, sql, Permission.class, args);
   }

   public static String hashPassword(Object salt, String password) throws ApiException
   {
      try
      {
         int iterationNb = 1000;
         MessageDigest digest = MessageDigest.getInstance("SHA-512");
         digest.reset();
         digest.update(salt.toString().getBytes());
         byte[] input = digest.digest(password.getBytes("UTF-8"));
         for (int i = 0; i < iterationNb; i++)
         {
            digest.reset();
            input = digest.digest(input);
         }

         String encoded = Base64.encodeBase64String(input).trim();
         return encoded;
      }
      catch (Exception ex)
      {
         throw new ApiException(SC.SC_500_INTERNAL_SERVER_ERROR);
      }
   }

   private String md5(byte[] byteArr)
   {
      try
      {
         MessageDigest digest = MessageDigest.getInstance("MD5");
         digest.update(byteArr);
         byte[] bytes = digest.digest();

         return Hex.encodeHexString( bytes );
      }
      catch (Exception ex)
      {
         throw new RuntimeException(ex);
      }
   }

   //   void close(Connection conn)
   //   {
   //      if (conn != null)
   //      {
   //         try
   //         {
   //            conn.close();
   //         }
   //         catch (Exception ex)
   //         {
   //            ex.printStackTrace();
   //         }
   //      }
   //   }

   protected String newSessionId()
   {
      String id = UUID.randomUUID().toString();
      id = id.replace("-", "");
      return id;
   }

   public void setSessionMax(int sessionMax)
   {
      this.sessionMax = sessionMax;
   }

   public void setFailedMax(int failedMax)
   {
      this.failedMax = failedMax;
   }

   public void setFailedExp(int failedExp)
   {
      this.failedExp = failedExp;
   }

   public void setCollection(String collection)
   {
      this.collection = collection;
   }

   public void setAuthenticatedPerm(String authenticatedPerm)
   {
      this.authenticatedPerm = authenticatedPerm;
   }

   public void setSessionCache(AuthSessionCache sessionCache)
   {
      this.sessionCache = sessionCache;
   }

   public void setShouldTrackRequestTimes(boolean shouldTrackRequestTimes)
   {
      this.shouldTrackRequestTimes = shouldTrackRequestTimes;
   }

   public void setSessionExp(long sessionExp)
   {
      this.sessionExp = sessionExp;
   }

   public void setDb(SqlDb db)
   {
      this.db = db;
   }

   class LRUAuthSessionCache implements AuthSessionCache
   {
      LRUMap map;

      public LRUAuthSessionCache(int sessionMax)
      {
         super();
         this.map = new LRUMap(sessionMax);;
      }

      @Override
      public User get(String sessionKey)
      {
         return (User) map.get(sessionKey);
      }

      @Override
      public void put(String sessionKey, User user)
      {
         map.put(sessionKey, user);
      }

      @Override
      public void remove(String sessionKey)
      {
         map.remove(sessionKey);
      }

   }

}
