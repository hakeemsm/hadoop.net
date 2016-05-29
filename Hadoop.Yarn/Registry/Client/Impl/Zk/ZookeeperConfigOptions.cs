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
using Org.Apache.Zookeeper.Client;
using Org.Apache.Zookeeper.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Impl.ZK
{
	/// <summary>
	/// Configuration options which are internal to Zookeeper,
	/// as well as some other ZK constants
	/// <p>
	/// Zookeeper options are passed via system properties prior to the ZK
	/// Methods/classes being invoked.
	/// </summary>
	/// <remarks>
	/// Configuration options which are internal to Zookeeper,
	/// as well as some other ZK constants
	/// <p>
	/// Zookeeper options are passed via system properties prior to the ZK
	/// Methods/classes being invoked. This implies that:
	/// <ol>
	/// <li>There can only be one instance of a ZK client or service class
	/// in a single JVM —else their configuration options will conflict.</li>
	/// <li>It is safest to set these properties immediately before
	/// invoking ZK operations.</li>
	/// </ol>
	/// </remarks>
	public abstract class ZookeeperConfigOptions
	{
		/// <summary>
		/// Enable SASL secure clients:
		/// <value/>
		/// .
		/// This is usually set to true, with ZK set to fall back to
		/// non-SASL authentication if the SASL auth fails
		/// by the property
		/// <see cref="PropZkServerMaintainConnectionDespiteSaslFailure"/>
		/// .
		/// <p>
		/// As a result, clients will default to attempting SASL-authentication,
		/// but revert to classic authentication/anonymous access on failure.
		/// </summary>
		public const string PropZkEnableSaslClient = "zookeeper.sasl.client";

		/// <summary>
		/// Default flag for the ZK client:
		/// <value/>
		/// .
		/// </summary>
		public const string DefaultZkEnableSaslClient = "true";

		/// <summary>
		/// System property for the JAAS client context :
		/// <value/>
		/// .
		/// For SASL authentication to work, this must point to a
		/// context within the
		/// <p>
		/// Default value is derived from
		/// <see cref="Org.Apache.Zookeeper.Client.ZooKeeperSaslClient.LoginContextNameKey"/>
		/// </summary>
		public const string PropZkSaslClientContext = ZooKeeperSaslClient.LoginContextNameKey;

		/// <summary>
		/// The SASL client username:
		/// <value/>
		/// .
		/// <p>
		/// Set this to the <i>short</i> name of the client, e.g, "user",
		/// not
		/// <c>user/host</c>
		/// , or
		/// <c>user/host@REALM</c>
		/// </summary>
		public const string PropZkSaslClientUsername = "zookeeper.sasl.client.username";

		/// <summary>
		/// The SASL Server context, referring to a context in the JVM's
		/// JAAS context file:
		/// <value/>
		/// </summary>
		public const string PropZkServerSaslContext = ZooKeeperSaslServer.LoginContextNameKey;

		/// <summary>
		/// Should ZK Server allow failed SASL clients to downgrade to classic
		/// authentication on a SASL auth failure:
		/// <value/>
		/// .
		/// </summary>
		public const string PropZkServerMaintainConnectionDespiteSaslFailure = "zookeeper.maintain_connection_despite_sasl_failure";

		/// <summary>
		/// should the ZK Server Allow failed SASL clients:
		/// <value/>
		/// .
		/// </summary>
		public const string PropZkAllowFailedSaslClients = "zookeeper.allowSaslFailedClients";

		/// <summary>
		/// Kerberos realm of the server:
		/// <value/>
		/// .
		/// </summary>
		public const string PropZkServerRealm = "zookeeper.server.realm";

		/// <summary>
		/// Path to a kinit binary:
		/// <value/>
		/// .
		/// Defaults to <code>"/usr/bin/kinit"</code>
		/// </summary>
		public const string PropZkKinitPath = "zookeeper.kinit";

		/// <summary>
		/// ID scheme for SASL:
		/// <value/>
		/// .
		/// </summary>
		public const string SchemeSasl = "sasl";

		/// <summary>
		/// ID scheme for digest auth:
		/// <value/>
		/// .
		/// </summary>
		public const string SchemeDigest = "digest";
	}

	public static class ZookeeperConfigOptionsConstants
	{
	}
}
