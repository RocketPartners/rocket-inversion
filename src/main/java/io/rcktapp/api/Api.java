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
package io.rcktapp.api;

import io.rocketpartners.cloud.action.security.AclRule;
import io.rocketpartners.cloud.model.Action;
import io.rocketpartners.cloud.model.ApiException;
import io.rocketpartners.cloud.model.Collection;
import io.rocketpartners.cloud.model.Db;
import io.rocketpartners.cloud.model.Endpoint;
import io.rocketpartners.cloud.model.Entity;
import io.rocketpartners.cloud.model.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static io.rocketpartners.cloud.model.SC.SC_404_NOT_FOUND;

public class Api extends Dto
{
    protected String name = null;
    protected String  apiCode     = null;
    protected String  accountCode = null;
    protected boolean multiTenant = false;
    protected String        url       = null;
    protected List<Db>      dbs       = new ArrayList<>();
    protected Set<Endpoint> endpoints = new TreeSet<>(Comparator.comparingInt(Rule::getOrder));
    protected List<Action>     actions     = new ArrayList<>();
    protected List<AclRule>    aclRules    = new ArrayList<>();
    protected List<Collection> collections = new ArrayList<>();
    protected String           hash        = null;
    boolean debug = false;
    transient long   loadTime = 0;
    transient Hashtable<String, Integer> cache = new Hashtable<>();

    public Api()
    {
    }

    public Api(String name)
    {
        this.name = name;
    }

    public void startup()
    {
        for (Db db : dbs)
        {
            db.startup();
        }
    }

    public void shutdown()
    {
        for (Db db : dbs)
        {
            db.shutdown();
        }
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getHash()
    {
        return hash;
    }

    public void setHash(String hash)
    {
        this.hash = hash;
    }

    public Table findTable(String name)
    {
        for (Db db : dbs)
        {
            Table t = db.getTable(name);
            if (t != null)
                return t;
        }
        return null;
    }

    public Db findDb(String collection, Class<?> dbClass)
    {
        Collection c = getCollection(collection, dbClass);
        if (c != null)
            return c.getEntity().getTable().getDb();

        return null;
    }

    public Collection getCollection(String name)
    {
        return getCollection(name, null);
    }

    public Collection getCollection(String name, Class<?> dbClass) throws ApiException
    {
        for (Collection collection : collections)
        {
            if (collection.getName().equalsIgnoreCase(name))
            {
                if (dbClass == null || dbClass.isAssignableFrom(collection.getEntity().getTable().getDb().getClass()))
                    return collection;
            }
        }

        for (Collection collection : collections)
        {
            // This loop is done separately from the one above to allow
            // collections to have precedence over aliases
            for (String alias : collection.getAliases())
            {
                if (name.equalsIgnoreCase(alias))
                {
                    if (dbClass == null || dbClass.isAssignableFrom(collection.getEntity().getTable().getDb().getClass()))
                        return collection;
                }
            }
        }

        if (dbClass != null)
        {
            throw new ApiException(SC_404_NOT_FOUND, "Collection '" + name + "' configured with Db class '" + dbClass.getSimpleName() + "' could not be found");
        }
        else
        {
            throw new ApiException(SC_404_NOT_FOUND, "Collection '" + name + "' could not be found");
        }
    }

    public Collection getCollection(Table tbl)
    {
        for (Collection collection : collections)
        {
            if (collection.getEntity().getTable() == tbl)
                return collection;
        }
        return null;
    }

    public Collection getCollection(Entity entity)
    {
        for (Collection collection : collections)
        {
            if (collection.getEntity() == entity)
                return collection;
        }
        return null;
    }

    public Entity getEntity(Table table)
    {
        for (Collection collection : collections)
        {
            if (collection.getEntity().getTable() == table)
                return collection.getEntity();
        }

        return null;
    }

    public List<Collection> getCollections()
    {
        return new ArrayList<>(collections);
    }

    public void setCollections(List<Collection> collections)
    {
        this.collections.clear();
        for (Collection collection : collections)
            addCollection(collection);
    }

    /**
     * Bidirectional method also sets 'this' api on the collection
     *
     * @param collection the collection to add to the collections list
     */
    public void addCollection(Collection collection)
    {
        if (!collections.contains(collection))
            collections.add(collection);

        if (collection.getApi() != this)
            collection.setApi(this);
    }

    public void removeCollection(Collection collection)
    {
        collections.remove(collection);
    }

    public Db getDb(String name)
    {
        if (name == null)
            return null;

        for (Db db : dbs)
        {
            if (name.equalsIgnoreCase(db.getName()))
                return db;
        }
        return null;
    }

    /**
     * @return the dbs
     */
    public List<Db> getDbs()
    {
        return new ArrayList<>(dbs);
    }

    /**
     * @param dbs the dbs to set
     */
    public void setDbs(List<Db> dbs)
    {
        for (Db db : dbs)
            addDb(db);
    }

    public void addDb(Db db)
    {
        if (!dbs.contains(db))
            dbs.add(db);

        if (db.getApi() != this)
            db.setApi(this);
    }

    public long getLoadTime()
    {
        return loadTime;
    }

    public void setLoadTime(long loadTime)
    {
        this.loadTime = loadTime;
    }

    public List<Endpoint> getEndpoints()
    {
        return new ArrayList<>(endpoints);
    }

    public void setEndpoints(List<Endpoint> endpoints)
    {
        this.endpoints.clear();
        for (Endpoint endpoint : endpoints)
            addEndpoint(endpoint);
    }

    public void addEndpoint(Endpoint endpoint)
    {
        endpoints.add(endpoint);

        if (endpoint.getApi() != this)
            endpoint.setApi(this);
    }

    public List<Action> getActions()
    {
        return new ArrayList<>(actions);
    }

    public void setActions(List<Action> actions)
    {
        this.actions.clear();
        for (Action action : actions)
            addAction(action);
    }

    public void addAction(Action action)
    {
        if (!actions.contains(action))
            actions.add(action);

        if (action.getApi() != this)
            action.setApi(this);
    }

    public void addAclRule(AclRule acl)
    {
        if (!aclRules.contains(acl))
        {
            aclRules.add(acl);
            Collections.sort(aclRules);
        }

        if (acl.getApi() != this)
            acl.setApi(this);
    }

    public List<AclRule> getAclRules()
    {
        return new ArrayList<>(aclRules);
    }

    public void setAclRules(List<AclRule> acls)
    {
        this.aclRules.clear();
        for (AclRule acl : acls)
            addAclRule(acl);
    }

    public boolean isDebug()
    {
        return debug;
    }

    public void setDebug(boolean debug)
    {
        this.debug = debug;
    }

    public String getApiCode()
    {
        return apiCode;
    }

    public void setApiCode(String apiCode)
    {
        this.apiCode = apiCode;
    }

    public String getAccountCode()
    {
        return accountCode;
    }

    public void setAccountCode(String accountCode)
    {
        this.accountCode = accountCode;
    }

    public boolean isMultiTenant()
    {
        return multiTenant;
    }

    public void setMultiTenant(boolean multiTenant)
    {
        this.multiTenant = multiTenant;
    }

    public void putCache(String key, Integer value)
    {
        cache.put(key, value);
    }

    public Integer getCache(String key)
    {
        return cache.get(key);
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

}
