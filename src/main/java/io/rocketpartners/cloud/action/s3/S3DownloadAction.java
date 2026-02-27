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
package io.rocketpartners.cloud.action.s3;

import io.rocketpartners.cloud.model.Action;

/**
 * Accepts RQL parameters and responds with json or files to the client.
 * Special request parameters used by the GET handler: 
 * 'download' attempts to download the specified key.
 * 'marker' determines where paging should begin
 * 
 * Supports simple RQL functions: eq & sw
 * 
 * TODO it would be awesome if a user could request several files to be downloaded.
 * The files would be zipped and returned to the client.  A zip would be named
 * either 'files.zip' for various files, or 'sw_x_files.zip' where files that
 * 'start with' x are zipped.
 * 
 * TODO what to do about buckets containing '.'s within the name? ex:
 * Missing parent for map compression: api.collections.s3db_files.liftck.coms
 * Missing parent for map compression: api.collections.s3db_static-pages.liftck.coms
 * Missing parent for map compression: s3db.tables.files.liftck.com
 * Missing parent for map compression: s3db.tables.static-pages.liftck.com
 * 
 * Mar 6, 2019 - If a json body is received, it is expected that a meta update should
 * occur.  If a multipart form is received, it is expected that a binary file
 * was sent and possibly json
 * 
 * @author kfrankic
 *
 */
public class S3DownloadAction extends Action
{

}
