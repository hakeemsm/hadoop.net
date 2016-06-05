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
using Org.Apache.Zookeeper;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Impl.ZK
{
	/// <summary>Internal constants for the registry.</summary>
	/// <remarks>
	/// Internal constants for the registry.
	/// These are the things which aren't visible to users.
	/// </remarks>
	public abstract class RegistryInternalConstants
	{
		/// <summary>Pattern of a single entry in the registry path.</summary>
		/// <remarks>
		/// Pattern of a single entry in the registry path. :
		/// <value/>
		/// .
		/// <p>
		/// This is what constitutes a valid hostname according to current RFCs.
		/// Alphanumeric first two and last one digit, alphanumeric
		/// and hyphens allowed in between.
		/// <p>
		/// No upper limit is placed on the size of an entry.
		/// </remarks>
		public const string ValidPathEntryPattern = "([a-z0-9]|[a-z0-9][a-z0-9\\-]*[a-z0-9])";

		/// <summary>
		/// Permissions for readers:
		/// <value/>
		/// .
		/// </summary>
		public const int PermissionsRegistryReaders = ZooDefs.Perms.Read;

		/// <summary>
		/// Permissions for system services:
		/// <value/>
		/// </summary>
		public const int PermissionsRegistrySystemServices = ZooDefs.Perms.All;

		/// <summary>
		/// Permissions for a user's root entry:
		/// <value/>
		/// .
		/// All except the admin permissions (ACL access) on a node
		/// </summary>
		public const int PermissionsRegistryUserRoot = ZooDefs.Perms.Read | ZooDefs.Perms
			.Write | ZooDefs.Perms.Create | ZooDefs.Perms.Delete;

		/// <summary>
		/// Name of the SASL auth provider which has to be added to ZK server to enable
		/// sasl: auth patterns:
		/// <value/>
		/// .
		/// Without this callers can connect via SASL, but
		/// they can't use it in ACLs
		/// </summary>
		public const string SaslauthenticationProvider = "org.apache.zookeeper.server.auth.SASLAuthenticationProvider";

		/// <summary>
		/// String to use as the prefix when declaring a new auth provider:
		/// <value/>
		/// .
		/// </summary>
		public const string ZookeeperAuthProvider = "zookeeper.authProvider";

		/// <summary>
		/// This the Hadoop environment variable which propagates the identity
		/// of a user in an insecure cluster
		/// </summary>
		public const string HadoopUserName = "HADOOP_USER_NAME";
	}

	public static class RegistryInternalConstantsConstants
	{
	}
}
