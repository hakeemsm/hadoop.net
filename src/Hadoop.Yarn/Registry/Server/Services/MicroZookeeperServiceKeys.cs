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
using Org.Apache.Hadoop.Registry.Client.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Server.Services
{
	/// <summary>
	/// Service keys for configuring the
	/// <see cref="MicroZookeeperService"/>
	/// .
	/// These are not used in registry clients or the RM-side service,
	/// so are kept separate.
	/// </summary>
	public abstract class MicroZookeeperServiceKeys
	{
		public const string ZkservicePrefix = RegistryConstants.RegistryPrefix + "zk.service.";

		/// <summary>
		/// Key to define the JAAS context for the ZK service:
		/// <value/>
		/// .
		/// </summary>
		public const string KeyRegistryZkserviceJaasContext = ZkservicePrefix + "service.jaas.context";

		/// <summary>
		/// ZK servertick time:
		/// <value/>
		/// </summary>
		public const string KeyZkserviceTickTime = ZkservicePrefix + "ticktime";

		/// <summary>
		/// host to register on:
		/// <value/>
		/// .
		/// </summary>
		public const string KeyZkserviceHost = ZkservicePrefix + "host";

		/// <summary>
		/// Default host to serve on -this is <code>localhost</code> as it
		/// is the only one guaranteed to be available:
		/// <value/>
		/// .
		/// </summary>
		public const string DefaultZkserviceHost = "localhost";

		/// <summary>
		/// port; 0 or below means "any":
		/// <value/>
		/// </summary>
		public const string KeyZkservicePort = ZkservicePrefix + "port";

		/// <summary>
		/// Directory containing data:
		/// <value/>
		/// </summary>
		public const string KeyZkserviceDir = ZkservicePrefix + "dir";

		/// <summary>
		/// Should failed SASL clients be allowed:
		/// <value/>
		/// ?
		/// Default is the ZK default: true
		/// </summary>
		public const string KeyZkserviceAllowFailedSaslClients = ZkservicePrefix + "allow.failed.sasl.clients";
	}

	public static class MicroZookeeperServiceKeysConstants
	{
	}
}
