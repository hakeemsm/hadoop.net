/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System.Collections.Generic;
using Org.Apache.Hadoop.Registry.Client.Types;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Api
{
	/// <summary>Registry Operations</summary>
	public interface RegistryOperations : Org.Apache.Hadoop.Service.Service
	{
		/// <summary>Create a path.</summary>
		/// <remarks>
		/// Create a path.
		/// It is not an error if the path exists already, be it empty or not.
		/// The createParents flag also requests creating the parents.
		/// As entries in the registry can hold data while still having
		/// child entries, it is not an error if any of the parent path
		/// elements have service records.
		/// </remarks>
		/// <param name="path">path to create</param>
		/// <param name="createParents">also create the parents.</param>
		/// <exception cref="Org.Apache.Hadoop.FS.PathNotFoundException">parent path is not in the registry.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	">path name is invalid.</exception>
		/// <exception cref="System.IO.IOException">Any other IO Exception.</exception>
		/// <returns>true if the path was created, false if it existed.</returns>
		bool Mknode(string path, bool createParents);

		/// <summary>Bind a path in the registry to a service record</summary>
		/// <param name="path">path to service record</param>
		/// <param name="record">service record service record to create/update</param>
		/// <param name="flags">bind flags</param>
		/// <exception cref="Org.Apache.Hadoop.FS.PathNotFoundException">the parent path does not exist
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException">
		/// path exists but create flags
		/// do not include "overwrite"
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	">path name is invalid.</exception>
		/// <exception cref="System.IO.IOException">Any other IO Exception.</exception>
		void Bind(string path, ServiceRecord record, int flags);

		/// <summary>Resolve the record at a path</summary>
		/// <param name="path">
		/// path to an entry containing a
		/// <see cref="Org.Apache.Hadoop.Registry.Client.Types.ServiceRecord"/>
		/// </param>
		/// <returns>the record</returns>
		/// <exception cref="Org.Apache.Hadoop.FS.PathNotFoundException">path is not in the registry.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.NoRecordException">if there is not a service record
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidRecordException
		/// 	">
		/// if there was a service record but it could
		/// not be parsed.
		/// </exception>
		/// <exception cref="System.IO.IOException">Any other IO Exception</exception>
		ServiceRecord Resolve(string path);

		/// <summary>Get the status of a path</summary>
		/// <param name="path">path to query</param>
		/// <returns>the status of the path</returns>
		/// <exception cref="Org.Apache.Hadoop.FS.PathNotFoundException">path is not in the registry.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	">the path is invalid.</exception>
		/// <exception cref="System.IO.IOException">Any other IO Exception</exception>
		RegistryPathStatus Stat(string path);

		/// <summary>Probe for a path existing.</summary>
		/// <remarks>
		/// Probe for a path existing.
		/// This is equivalent to
		/// <see cref="Stat(string)"/>
		/// with
		/// any failure downgraded to a
		/// </remarks>
		/// <param name="path">path to query</param>
		/// <returns>true if the path was found</returns>
		/// <exception cref="System.IO.IOException"/>
		bool Exists(string path);

		/// <summary>
		/// List all entries under a registry path, returning the relative names
		/// of the entries.
		/// </summary>
		/// <param name="path">path to query</param>
		/// <returns>
		/// a possibly empty list of the short path names of
		/// child entries.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.FS.PathNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		IList<string> List(string path);

		/// <summary>Delete a path.</summary>
		/// <remarks>
		/// Delete a path.
		/// If the operation returns without an error then the entry has been
		/// deleted.
		/// </remarks>
		/// <param name="path">path delete recursively</param>
		/// <param name="recursive">recursive flag</param>
		/// <exception cref="Org.Apache.Hadoop.FS.PathNotFoundException">path is not in the registry.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	">the path is invalid.</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.PathIsNotEmptyDirectoryException">
		/// path has child entries, but
		/// recursive is false.
		/// </exception>
		/// <exception cref="System.IO.IOException">Any other IO Exception</exception>
		void Delete(string path, bool recursive);

		/// <summary>
		/// Add a new write access entry to be added to node permissions in all
		/// future write operations of a session connected to a secure registry.
		/// </summary>
		/// <remarks>
		/// Add a new write access entry to be added to node permissions in all
		/// future write operations of a session connected to a secure registry.
		/// This does not grant the session any more rights: if it lacked any write
		/// access, it will still be unable to manipulate the registry.
		/// In an insecure cluster, this operation has no effect.
		/// </remarks>
		/// <param name="id">ID to use</param>
		/// <param name="pass">password</param>
		/// <returns>
		/// true if the accessor was added: that is, the registry connection
		/// uses permissions to manage access
		/// </returns>
		/// <exception cref="System.IO.IOException">on any failure to build the digest</exception>
		bool AddWriteAccessor(string id, string pass);

		/// <summary>Clear all write accessors.</summary>
		/// <remarks>
		/// Clear all write accessors.
		/// At this point all standard permissions/ACLs are retained,
		/// including any set on behalf of the user
		/// Only  accessors added via
		/// <see cref="AddWriteAccessor(string, string)"/>
		/// are removed.
		/// </remarks>
		void ClearWriteAccessors();
	}
}
