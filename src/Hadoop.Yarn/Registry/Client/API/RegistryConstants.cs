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
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Api
{
	/// <summary>
	/// Constants for the registry, including configuration keys and default
	/// values.
	/// </summary>
	public abstract class RegistryConstants
	{
		/// <summary>
		/// prefix for registry configuration options:
		/// <value/>
		/// .
		/// Why <code>hadoop.</code> and not YARN? It can
		/// live outside YARN
		/// </summary>
		public const string RegistryPrefix = "hadoop.registry.";

		/// <summary>
		/// Prefix for zookeeper-specific options:
		/// <value/>
		/// <p>
		/// For clients using other protocols, these options are not supported.
		/// </summary>
		public const string ZkPrefix = RegistryPrefix + "zk.";

		/// <summary>
		/// flag to indicate whether or not the registry should
		/// be enabled in the RM:
		/// <value/>
		/// </summary>
		public const string KeyRegistryEnabled = RegistryPrefix + "rm.enabled";

		/// <summary>
		/// Defaut value for enabling the registry in the RM:
		/// <value/>
		/// </summary>
		public const bool DefaultRegistryEnabled = false;

		/// <summary>
		/// Key to set if the registry is secure:
		/// <value/>
		/// .
		/// Turning it on changes the permissions policy from "open access"
		/// to restrictions on kerberos with the option of
		/// a user adding one or more auth key pairs down their
		/// own tree.
		/// </summary>
		public const string KeyRegistrySecure = RegistryPrefix + "secure";

		/// <summary>
		/// Default registry security policy:
		/// <value/>
		/// .
		/// </summary>
		public const bool DefaultRegistrySecure = false;

		/// <summary>
		/// Root path in the ZK tree for the registry:
		/// <value/>
		/// </summary>
		public const string KeyRegistryZkRoot = ZkPrefix + "root";

		/// <summary>
		/// Default root of the yarn registry:
		/// <value/>
		/// </summary>
		public const string DefaultZkRegistryRoot = "/registry";

		/// <summary>Registry client authentication policy.</summary>
		/// <remarks>
		/// Registry client authentication policy.
		/// <p>
		/// This is only used in secure clusters.
		/// <p>
		/// If the Factory methods of
		/// <see cref="RegistryOperationsFactory"/>
		/// are used, this key does not need to be set: it is set
		/// up based on the factory method used.
		/// </remarks>
		public const string KeyRegistryClientAuth = RegistryPrefix + "client.auth";

		/// <summary>
		/// Registry client uses Kerberos: authentication is automatic from
		/// logged in user
		/// </summary>
		public const string RegistryClientAuthKerberos = "kerberos";

		/// <summary>Username/password is the authentication mechanism.</summary>
		/// <remarks>
		/// Username/password is the authentication mechanism.
		/// If set then both
		/// <see cref="KeyRegistryClientAuthenticationId"/>
		/// and
		/// <see cref="KeyRegistryClientAuthenticationPassword"/>
		/// must be set.
		/// </remarks>
		public const string RegistryClientAuthDigest = "digest";

		/// <summary>No authentication; client is anonymous</summary>
		public const string RegistryClientAuthAnonymous = string.Empty;

		/// <summary>
		/// Registry client authentication ID
		/// <p>
		/// This is only used in secure clusters with
		/// <see cref="KeyRegistryClientAuth"/>
		/// set to
		/// <see cref="RegistryClientAuthDigest"/>
		/// </summary>
		public const string KeyRegistryClientAuthenticationId = KeyRegistryClientAuth + ".id";

		/// <summary>Registry client authentication password.</summary>
		/// <remarks>
		/// Registry client authentication password.
		/// <p>
		/// This is only used in secure clusters with the client set to
		/// use digest (not SASL or anonymouse) authentication.
		/// <p>
		/// Specifically,
		/// <see cref="KeyRegistryClientAuth"/>
		/// set to
		/// <see cref="RegistryClientAuthDigest"/>
		/// </remarks>
		public const string KeyRegistryClientAuthenticationPassword = KeyRegistryClientAuth
			 + ".password";

		/// <summary>
		/// List of hostname:port pairs defining the
		/// zookeeper quorum binding for the registry
		/// <value/>
		/// </summary>
		public const string KeyRegistryZkQuorum = ZkPrefix + "quorum";

		/// <summary>
		/// The default zookeeper quorum binding for the registry:
		/// <value/>
		/// </summary>
		public const string DefaultRegistryZkQuorum = "localhost:2181";

		/// <summary>
		/// Zookeeper session timeout in milliseconds:
		/// <value/>
		/// </summary>
		public const string KeyRegistryZkSessionTimeout = ZkPrefix + "session.timeout.ms";

		/// <summary>
		/// The default ZK session timeout:
		/// <value/>
		/// .
		/// </summary>
		public const int DefaultZkSessionTimeout = 60000;

		/// <summary>
		/// Zookeeper connection timeout in milliseconds:
		/// <value/>
		/// .
		/// </summary>
		public const string KeyRegistryZkConnectionTimeout = ZkPrefix + "connection.timeout.ms";

		/// <summary>
		/// The default ZK connection timeout:
		/// <value/>
		/// .
		/// </summary>
		public const int DefaultZkConnectionTimeout = 15000;

		/// <summary>
		/// Zookeeper connection retry count before failing:
		/// <value/>
		/// .
		/// </summary>
		public const string KeyRegistryZkRetryTimes = ZkPrefix + "retry.times";

		/// <summary>
		/// The default # of times to retry a ZK connection:
		/// <value/>
		/// .
		/// </summary>
		public const int DefaultZkRetryTimes = 5;

		/// <summary>
		/// Zookeeper connect interval in milliseconds:
		/// <value/>
		/// .
		/// </summary>
		public const string KeyRegistryZkRetryInterval = ZkPrefix + "retry.interval.ms";

		/// <summary>
		/// The default interval between connection retries:
		/// <value/>
		/// .
		/// </summary>
		public const int DefaultZkRetryInterval = 1000;

		/// <summary>
		/// Zookeeper retry limit in milliseconds, during
		/// exponential backoff:
		/// <value/>
		/// .
		/// This places a limit even
		/// if the retry times and interval limit, combined
		/// with the backoff policy, result in a long retry
		/// period
		/// </summary>
		public const string KeyRegistryZkRetryCeiling = ZkPrefix + "retry.ceiling.ms";

		/// <summary>
		/// Default limit on retries:
		/// <value/>
		/// .
		/// </summary>
		public const int DefaultZkRetryCeiling = 60000;

		/// <summary>
		/// A comma separated list of Zookeeper ACL identifiers with
		/// system access to the registry in a secure cluster:
		/// <value/>
		/// .
		/// These are given full access to all entries.
		/// If there is an "@" at the end of an entry it
		/// instructs the registry client to append the kerberos realm as
		/// derived from the login and
		/// <see cref="KeyRegistryKerberosRealm"/>
		/// .
		/// </summary>
		public const string KeyRegistrySystemAccounts = RegistryPrefix + "system.accounts";

		/// <summary>
		/// Default system accounts given global access to the registry:
		/// <value/>
		/// .
		/// </summary>
		public const string DefaultRegistrySystemAccounts = "sasl:yarn@, sasl:mapred@, sasl:hdfs@, sasl:hadoop@";

		/// <summary>
		/// A comma separated list of Zookeeper ACL identifiers with
		/// system access to the registry in a secure cluster:
		/// <value/>
		/// .
		/// These are given full access to all entries.
		/// If there is an "@" at the end of an entry it
		/// instructs the registry client to append the default kerberos domain.
		/// </summary>
		public const string KeyRegistryUserAccounts = RegistryPrefix + "user.accounts";

		/// <summary>
		/// Default system acls:
		/// <value/>
		/// .
		/// </summary>
		public const string DefaultRegistryUserAccounts = string.Empty;

		/// <summary>
		/// The kerberos realm:
		/// <value/>
		/// .
		/// This is used to set the realm of
		/// system principals which do not declare their realm,
		/// and any other accounts that need the value.
		/// If empty, the default realm of the running process
		/// is used.
		/// If neither are known and the realm is needed, then the registry
		/// service/client will fail.
		/// </summary>
		public const string KeyRegistryKerberosRealm = RegistryPrefix + "kerberos.realm";

		/// <summary>Key to define the JAAS context.</summary>
		/// <remarks>
		/// Key to define the JAAS context. Used in secure registries:
		/// <value/>
		/// .
		/// </remarks>
		public const string KeyRegistryClientJaasContext = RegistryPrefix + "jaas.context";

		/// <summary>
		/// default client-side registry JAAS context:
		/// <value/>
		/// </summary>
		public const string DefaultRegistryClientJaasContext = "Client";

		/// <summary>
		/// path to users off the root:
		/// <value/>
		/// .
		/// </summary>
		public const string PathUsers = "/users/";

		/// <summary>
		/// path to system services off the root :
		/// <value/>
		/// .
		/// </summary>
		public const string PathSystemServices = "/services/";

		/// <summary>
		/// path to system services under a user's home path :
		/// <value/>
		/// .
		/// </summary>
		public const string PathUserServices = "/services/";

		/// <summary>
		/// path under a service record to point to components of that service:
		/// <value/>
		/// .
		/// </summary>
		public const string SubpathComponents = "/components/";
	}

	public static class RegistryConstantsConstants
	{
	}
}
