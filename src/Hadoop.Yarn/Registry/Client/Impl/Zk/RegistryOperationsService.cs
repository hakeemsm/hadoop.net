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
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Client.Binding;
using Org.Apache.Hadoop.Registry.Client.Types;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Impl.ZK
{
	/// <summary>The Registry operations service.</summary>
	/// <remarks>
	/// The Registry operations service.
	/// <p>
	/// This service implements the
	/// <see cref="Org.Apache.Hadoop.Registry.Client.Api.RegistryOperations"/>
	/// API by mapping the commands to zookeeper operations, and translating
	/// results and exceptions back into those specified by the API.
	/// <p>
	/// Factory methods should hide the detail that this has been implemented via
	/// the
	/// <see cref="CuratorService"/>
	/// by returning it cast to that
	/// <see cref="Org.Apache.Hadoop.Registry.Client.Api.RegistryOperations"/>
	/// interface, rather than this implementation class.
	/// </remarks>
	public class RegistryOperationsService : CuratorService, RegistryOperations
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Registry.Client.Impl.ZK.RegistryOperationsService
			));

		private readonly RegistryUtils.ServiceRecordMarshal serviceRecordMarshal = new RegistryUtils.ServiceRecordMarshal
			();

		public RegistryOperationsService(string name)
			: this(name, null)
		{
		}

		public RegistryOperationsService()
			: this("RegistryOperationsService")
		{
		}

		public RegistryOperationsService(string name, RegistryBindingSource bindingSource
			)
			: base(name, bindingSource)
		{
		}

		/// <summary>
		/// Get the aggregate set of ACLs the client should use
		/// to create directories
		/// </summary>
		/// <returns>the ACL list</returns>
		public virtual IList<ACL> GetClientAcls()
		{
			return GetRegistrySecurity().GetClientACLs();
		}

		/// <summary>Validate a path</summary>
		/// <param name="path">path to validate</param>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	">if a path is considered invalid</exception>
		protected internal virtual void ValidatePath(string path)
		{
		}

		// currently no checks are performed
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Mknode(string path, bool createParents)
		{
			ValidatePath(path);
			return ZkMkPath(path, CreateMode.Persistent, createParents, GetClientAcls());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Bind(string path, ServiceRecord record, int flags)
		{
			Preconditions.CheckArgument(record != null, "null record");
			ValidatePath(path);
			// validate the record before putting it
			RegistryTypeUtils.ValidateServiceRecord(path, record);
			Log.Info("Bound at {} : {}", path, record);
			CreateMode mode = CreateMode.Persistent;
			byte[] bytes = serviceRecordMarshal.ToBytes(record);
			ZkSet(path, mode, bytes, GetClientAcls(), ((flags & BindFlags.Overwrite) != 0));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ServiceRecord Resolve(string path)
		{
			byte[] bytes = ZkRead(path);
			ServiceRecord record = serviceRecordMarshal.FromBytes(path, bytes, ServiceRecord.
				RecordType);
			RegistryTypeUtils.ValidateServiceRecord(path, record);
			return record;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Exists(string path)
		{
			ValidatePath(path);
			return ZkPathExists(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RegistryPathStatus Stat(string path)
		{
			ValidatePath(path);
			Org.Apache.Zookeeper.Data.Stat stat = ZkStat(path);
			string name = RegistryPathUtils.LastPathEntry(path);
			RegistryPathStatus status = new RegistryPathStatus(name, stat.GetCtime(), stat.GetDataLength
				(), stat.GetNumChildren());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Stat {} => {}", path, status);
			}
			return status;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IList<string> List(string path)
		{
			ValidatePath(path);
			return ZkList(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Delete(string path, bool recursive)
		{
			ValidatePath(path);
			ZkDelete(path, recursive, null);
		}
	}
}
