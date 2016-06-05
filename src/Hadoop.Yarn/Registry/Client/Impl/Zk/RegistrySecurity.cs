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
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.Security.Auth.Login;
using Org.Apache.Commons.Lang;
using Org.Apache.Curator.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Org.Apache.Zookeeper.Server.Auth;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Impl.ZK
{
	/// <summary>Implement the registry security ...</summary>
	/// <remarks>
	/// Implement the registry security ... a self contained service for
	/// testability.
	/// <p>
	/// This class contains:
	/// <ol>
	/// <li>
	/// The registry security policy implementation, configuration reading, ACL
	/// setup and management
	/// </li>
	/// <li>Lots of static helper methods to aid security setup and debugging</li>
	/// </ol>
	/// </remarks>
	public class RegistrySecurity : AbstractService
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Registry.Client.Impl.ZK.RegistrySecurity
			));

		public const string EUnknownAuthenticationMechanism = "Unknown/unsupported authentication mechanism; ";

		/// <summary>
		/// there's no default user to add with permissions, so it would be
		/// impossible to create nodes with unrestricted user access
		/// </summary>
		public const string ENoUserDeterminedForAcls = "No user for ACLs determinable from current user or registry option "
			 + KeyRegistryUserAccounts;

		/// <summary>
		/// Error raised when the registry is tagged as secure but this
		/// process doesn't have hadoop security enabled.
		/// </summary>
		public const string ENoKerberos = "Registry security is enabled -but Hadoop security is not enabled";

		/// <summary>Access policy options</summary>
		private enum AccessPolicy
		{
			anon,
			sasl,
			digest
		}

		/// <summary>Access mechanism</summary>
		private RegistrySecurity.AccessPolicy access;

		/// <summary>User used for digest auth</summary>
		private string digestAuthUser;

		/// <summary>Password used for digest auth</summary>
		private string digestAuthPassword;

		/// <summary>Auth data used for digest auth</summary>
		private byte[] digestAuthData;

		/// <summary>flag set to true if the registry has security enabled.</summary>
		private bool secureRegistry;

		/// <summary>An ACL with read-write access for anyone</summary>
		public static readonly ACL AllReadwriteAccess = new ACL(ZooDefs.Perms.All, ZooDefs.Ids
			.AnyoneIdUnsafe);

		/// <summary>An ACL with read access for anyone</summary>
		public static readonly ACL AllReadAccess = new ACL(ZooDefs.Perms.Read, ZooDefs.Ids
			.AnyoneIdUnsafe);

		/// <summary>
		/// An ACL list containing the
		/// <see cref="AllReadwriteAccess"/>
		/// entry.
		/// It is copy on write so can be shared without worry
		/// </summary>
		public static readonly IList<ACL> WorldReadWriteACL;

		static RegistrySecurity()
		{
			IList<ACL> acls = new AList<ACL>();
			acls.AddItem(AllReadwriteAccess);
			WorldReadWriteACL = new CopyOnWriteArrayList<ACL>(acls);
		}

		/// <summary>the list of system ACLs</summary>
		private readonly IList<ACL> systemACLs = new AList<ACL>();

		/// <summary>
		/// A list of digest ACLs which can be added to permissions
		/// —and cleared later.
		/// </summary>
		private readonly IList<ACL> digestACLs = new AList<ACL>();

		/// <summary>the default kerberos realm</summary>
		private string kerberosRealm;

		/// <summary>Client context</summary>
		private string jaasClientContext;

		/// <summary>Client identity</summary>
		private string jaasClientIdentity;

		/// <summary>Create an instance</summary>
		/// <param name="name">service name</param>
		public RegistrySecurity(string name)
			: base(name)
		{
		}

		/// <summary>Init the service: this sets up security based on the configuration</summary>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			string auth = conf.GetTrimmed(KeyRegistryClientAuth, RegistryClientAuthAnonymous);
			switch (auth)
			{
				case RegistryClientAuthKerberos:
				{
					access = RegistrySecurity.AccessPolicy.sasl;
					break;
				}

				case RegistryClientAuthDigest:
				{
					access = RegistrySecurity.AccessPolicy.digest;
					break;
				}

				case RegistryClientAuthAnonymous:
				{
					access = RegistrySecurity.AccessPolicy.anon;
					break;
				}

				default:
				{
					throw new ServiceStateException(EUnknownAuthenticationMechanism + "\"" + auth + "\""
						);
				}
			}
			InitSecurity();
		}

		/// <summary>Init security.</summary>
		/// <remarks>
		/// Init security.
		/// After this operation, the
		/// <see cref="systemACLs"/>
		/// list is valid.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void InitSecurity()
		{
			secureRegistry = GetConfig().GetBoolean(KeyRegistrySecure, DefaultRegistrySecure);
			systemACLs.Clear();
			if (secureRegistry)
			{
				AddSystemACL(AllReadAccess);
				// determine the kerberos realm from JVM and settings
				kerberosRealm = GetConfig().Get(KeyRegistryKerberosRealm, GetDefaultRealmInJVM());
				// System Accounts
				string system = GetOrFail(KeyRegistrySystemAccounts, DefaultRegistrySystemAccounts
					);
				Sharpen.Collections.AddAll(systemACLs, BuildACLs(system, kerberosRealm, ZooDefs.Perms
					.All));
				// user accounts (may be empty, but for digest one user AC must
				// be built up
				string user = GetConfig().Get(KeyRegistryUserAccounts, DefaultRegistryUserAccounts
					);
				IList<ACL> userACLs = BuildACLs(user, kerberosRealm, ZooDefs.Perms.All);
				// add self if the current user can be determined
				ACL self;
				if (UserGroupInformation.IsSecurityEnabled())
				{
					self = CreateSaslACLFromCurrentUser(ZooDefs.Perms.All);
					if (self != null)
					{
						userACLs.AddItem(self);
					}
				}
				switch (access)
				{
					case RegistrySecurity.AccessPolicy.sasl:
					{
						// here check for UGI having secure on or digest + ID
						// secure + SASL => has to be authenticated
						if (!UserGroupInformation.IsSecurityEnabled())
						{
							throw new IOException("Kerberos required for secure registry access");
						}
						UserGroupInformation currentUser = UserGroupInformation.GetCurrentUser();
						jaasClientContext = GetOrFail(KeyRegistryClientJaasContext, DefaultRegistryClientJaasContext
							);
						jaasClientIdentity = currentUser.GetShortUserName();
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Auth is SASL user=\"{}\" JAAS context=\"{}\"", jaasClientIdentity, jaasClientContext
								);
						}
						break;
					}

					case RegistrySecurity.AccessPolicy.digest:
					{
						string id = GetOrFail(KeyRegistryClientAuthenticationId, string.Empty);
						string pass = GetOrFail(KeyRegistryClientAuthenticationPassword, string.Empty);
						if (userACLs.IsEmpty())
						{
							//
							throw new ServiceStateException(ENoUserDeterminedForAcls);
						}
						Digest(id, pass);
						ACL acl = new ACL(ZooDefs.Perms.All, ToDigestId(id, pass));
						userACLs.AddItem(acl);
						digestAuthUser = id;
						digestAuthPassword = pass;
						string authPair = id + ":" + pass;
						digestAuthData = Sharpen.Runtime.GetBytesForString(authPair, "UTF-8");
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Auth is Digest ACL: {}", AclToString(acl));
						}
						break;
					}

					case RegistrySecurity.AccessPolicy.anon:
					{
						// nothing is needed; account is read only.
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Auth is anonymous");
						}
						userACLs = new AList<ACL>(0);
						break;
					}
				}
				Sharpen.Collections.AddAll(systemACLs, userACLs);
			}
			else
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Registry has no security");
				}
				// wide open cluster, adding system acls
				Sharpen.Collections.AddAll(systemACLs, WorldReadWriteACL);
			}
		}

		/// <summary>Add another system ACL</summary>
		/// <param name="acl">add ACL</param>
		public virtual void AddSystemACL(ACL acl)
		{
			systemACLs.AddItem(acl);
		}

		/// <summary>Add a digest ACL</summary>
		/// <param name="acl">add ACL</param>
		public virtual bool AddDigestACL(ACL acl)
		{
			if (secureRegistry)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Added ACL {}", AclToString(acl));
				}
				digestACLs.AddItem(acl);
				return true;
			}
			else
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Ignoring added ACL - registry is insecure{}", AclToString(acl));
				}
				return false;
			}
		}

		/// <summary>Reset the digest ACL list</summary>
		public virtual void ResetDigestACLs()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Cleared digest ACLs");
			}
			digestACLs.Clear();
		}

		/// <summary>Flag to indicate the cluster is secure</summary>
		/// <returns>true if the config enabled security</returns>
		public virtual bool IsSecureRegistry()
		{
			return secureRegistry;
		}

		/// <summary>Get the system principals</summary>
		/// <returns>the system principals</returns>
		public virtual IList<ACL> GetSystemACLs()
		{
			Preconditions.CheckNotNull(systemACLs, "registry security is unitialized");
			return Sharpen.Collections.UnmodifiableList(systemACLs);
		}

		/// <summary>Get all ACLs needed for a client to use when writing to the repo.</summary>
		/// <remarks>
		/// Get all ACLs needed for a client to use when writing to the repo.
		/// That is: system ACLs, its own ACL, any digest ACLs
		/// </remarks>
		/// <returns>the client ACLs</returns>
		public virtual IList<ACL> GetClientACLs()
		{
			IList<ACL> clientACLs = new AList<ACL>(systemACLs);
			Sharpen.Collections.AddAll(clientACLs, digestACLs);
			return clientACLs;
		}

		/// <summary>Create a SASL ACL for the user</summary>
		/// <param name="perms">permissions</param>
		/// <returns>an ACL for the current user or null if they aren't a kerberos user</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual ACL CreateSaslACLFromCurrentUser(int perms)
		{
			UserGroupInformation currentUser = UserGroupInformation.GetCurrentUser();
			if (currentUser.HasKerberosCredentials())
			{
				return CreateSaslACL(currentUser, perms);
			}
			else
			{
				return null;
			}
		}

		/// <summary>Given a UGI, create a SASL ACL from it</summary>
		/// <param name="ugi">UGI</param>
		/// <param name="perms">permissions</param>
		/// <returns>a new ACL</returns>
		public virtual ACL CreateSaslACL(UserGroupInformation ugi, int perms)
		{
			string userName = ugi.GetUserName();
			return new ACL(perms, new ID(SchemeSasl, userName));
		}

		/// <summary>Get a conf option, throw an exception if it is null/empty</summary>
		/// <param name="key">key</param>
		/// <param name="defval">default value</param>
		/// <returns>the value</returns>
		/// <exception cref="System.IO.IOException">if missing</exception>
		private string GetOrFail(string key, string defval)
		{
			string val = GetConfig().Get(key, defval);
			if (StringUtils.IsEmpty(val))
			{
				throw new IOException("Missing value for configuration option " + key);
			}
			return val;
		}

		/// <summary>Check for an id:password tuple being valid.</summary>
		/// <remarks>
		/// Check for an id:password tuple being valid.
		/// This test is stricter than that in
		/// <see cref="Org.Apache.Zookeeper.Server.Auth.DigestAuthenticationProvider"/>
		/// ,
		/// which splits the string, but doesn't check the contents of each
		/// half for being non-"".
		/// </remarks>
		/// <param name="idPasswordPair">id:pass pair</param>
		/// <returns>true if the pass is considered valid.</returns>
		public virtual bool IsValid(string idPasswordPair)
		{
			string[] parts = idPasswordPair.Split(":");
			return parts.Length == 2 && !StringUtils.IsEmpty(parts[0]) && !StringUtils.IsEmpty
				(parts[1]);
		}

		/// <summary>Get the derived kerberos realm.</summary>
		/// <returns>
		/// this is built from the JVM realm, or the configuration if it
		/// overrides it. If "", it means "don't know".
		/// </returns>
		public virtual string GetKerberosRealm()
		{
			return kerberosRealm;
		}

		/// <summary>Generate a base-64 encoded digest of the idPasswordPair pair</summary>
		/// <param name="idPasswordPair">id:password</param>
		/// <returns>a string that can be used for authentication</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual string Digest(string idPasswordPair)
		{
			if (StringUtils.IsEmpty(idPasswordPair) || !IsValid(idPasswordPair))
			{
				throw new IOException("Invalid id:password: " + idPasswordPair);
			}
			try
			{
				return DigestAuthenticationProvider.GenerateDigest(idPasswordPair);
			}
			catch (NoSuchAlgorithmException e)
			{
				// unlikely since it is standard to the JVM, but maybe JCE restrictions
				// could trigger it
				throw new IOException(e.ToString(), e);
			}
		}

		/// <summary>Generate a base-64 encoded digest of the idPasswordPair pair</summary>
		/// <param name="id">ID</param>
		/// <param name="password">pass</param>
		/// <returns>a string that can be used for authentication</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual string Digest(string id, string password)
		{
			return Digest(id + ":" + password);
		}

		/// <summary>Given a digest, create an ID from it</summary>
		/// <param name="digest">digest</param>
		/// <returns>ID</returns>
		public virtual ID ToDigestId(string digest)
		{
			return new ID(SchemeDigest, digest);
		}

		/// <summary>Create a Digest ID from an id:pass pair</summary>
		/// <param name="id">ID</param>
		/// <param name="password">password</param>
		/// <returns>an ID</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual ID ToDigestId(string id, string password)
		{
			return ToDigestId(Digest(id, password));
		}

		/// <summary>
		/// Split up a list of the form
		/// <code>sasl:mapred@,digest:5f55d66, sasl@yarn@EXAMPLE.COM</code>
		/// into a list of possible ACL values, trimming as needed
		/// The supplied realm is added to entries where
		/// <ol>
		/// <li>the string begins "sasl:"</li>
		/// <li>the string ends with "@"</li>
		/// </ol>
		/// No attempt is made to validate any of the acl patterns.
		/// </summary>
		/// <param name="aclString">list of 0 or more ACLs</param>
		/// <param name="realm">realm to add</param>
		/// <returns>a list of split and potentially patched ACL pairs.</returns>
		public virtual IList<string> SplitAclPairs(string aclString, string realm)
		{
			IList<string> list = Lists.NewArrayList(Splitter.On(',').OmitEmptyStrings().TrimResults
				().Split(aclString));
			ListIterator<string> listIterator = list.ListIterator();
			while (listIterator.HasNext())
			{
				string next = listIterator.Next();
				if (next.StartsWith(SchemeSasl + ":") && next.EndsWith("@"))
				{
					listIterator.Set(next + realm);
				}
			}
			return list;
		}

		/// <summary>Parse a string down to an ID, adding a realm if needed</summary>
		/// <param name="idPair">id:data tuple</param>
		/// <param name="realm">realm to add</param>
		/// <returns>the ID.</returns>
		/// <exception cref="System.ArgumentException">if the idPair is invalid</exception>
		public virtual ID Parse(string idPair, string realm)
		{
			int firstColon = idPair.IndexOf(':');
			int lastColon = idPair.LastIndexOf(':');
			if (firstColon == -1 || lastColon == -1 || firstColon != lastColon)
			{
				throw new ArgumentException("ACL '" + idPair + "' not of expected form scheme:id"
					);
			}
			string scheme = Sharpen.Runtime.Substring(idPair, 0, firstColon);
			string id = Sharpen.Runtime.Substring(idPair, firstColon + 1);
			if (id.EndsWith("@"))
			{
				Preconditions.CheckArgument(StringUtils.IsNotEmpty(realm), "@ suffixed account but no realm %s"
					, id);
				id = id + realm;
			}
			return new ID(scheme, id);
		}

		/// <summary>Parse the IDs, adding a realm if needed, setting the permissions</summary>
		/// <param name="principalList">id string</param>
		/// <param name="realm">realm to add</param>
		/// <param name="perms">permissions</param>
		/// <returns>the relevant ACLs</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual IList<ACL> BuildACLs(string principalList, string realm, int perms
			)
		{
			IList<string> aclPairs = SplitAclPairs(principalList, realm);
			IList<ACL> ids = new AList<ACL>(aclPairs.Count);
			foreach (string aclPair in aclPairs)
			{
				ACL newAcl = new ACL();
				newAcl.SetId(Parse(aclPair, realm));
				newAcl.SetPerms(perms);
				ids.AddItem(newAcl);
			}
			return ids;
		}

		/// <summary>Parse an ACL list.</summary>
		/// <remarks>
		/// Parse an ACL list. This includes configuration indirection
		/// <see cref="Org.Apache.Hadoop.Util.ZKUtil.ResolveConfIndirection(string)"/>
		/// </remarks>
		/// <param name="zkAclConf">configuration string</param>
		/// <returns>an ACL list</returns>
		/// <exception cref="System.IO.IOException">on a bad ACL parse</exception>
		public virtual IList<ACL> ParseACLs(string zkAclConf)
		{
			try
			{
				return ZKUtil.ParseACLs(ZKUtil.ResolveConfIndirection(zkAclConf));
			}
			catch (ZKUtil.BadAclFormatException e)
			{
				throw new IOException("Parsing " + zkAclConf + " :" + e, e);
			}
		}

		/// <summary>
		/// Get the appropriate Kerberos Auth module for JAAS entries
		/// for this JVM.
		/// </summary>
		/// <returns>a JVM-specific kerberos login module classname.</returns>
		public static string GetKerberosAuthModuleForJVM()
		{
			if (Runtime.GetProperty("java.vendor").Contains("IBM"))
			{
				return "com.ibm.security.auth.module.Krb5LoginModule";
			}
			else
			{
				return "com.sun.security.auth.module.Krb5LoginModule";
			}
		}

		/// <summary>
		/// JAAS template:
		/// <value/>
		/// Note the semicolon on the last entry
		/// </summary>
		private const string JaasEntry = "%s { %n" + " %s required%n" + " keyTab=\"%s\"%n"
			 + " debug=true%n" + " principal=\"%s\"%n" + " useKeyTab=true%n" + " useTicketCache=false%n"
			 + " doNotPrompt=true%n" + " storeKey=true;%n" + "}; %n";

		// kerberos module
		/// <summary>Create a JAAS entry for insertion</summary>
		/// <param name="context">context of the entry</param>
		/// <param name="principal">kerberos principal</param>
		/// <param name="keytab">keytab</param>
		/// <returns>a context</returns>
		public virtual string CreateJAASEntry(string context, string principal, FilePath 
			keytab)
		{
			Preconditions.CheckArgument(StringUtils.IsNotEmpty(principal), "invalid principal"
				);
			Preconditions.CheckArgument(StringUtils.IsNotEmpty(context), "invalid context");
			Preconditions.CheckArgument(keytab != null && keytab.IsFile(), "Keytab null or missing: "
				);
			string keytabpath = keytab.GetAbsolutePath();
			// fix up for windows; no-op on unix
			keytabpath = keytabpath.Replace('\\', '/');
			return string.Format(Sharpen.Extensions.GetEnglishCulture(), JaasEntry, context, 
				GetKerberosAuthModuleForJVM(), keytabpath, principal);
		}

		/// <summary>Bind the JVM JAS setting to the specified JAAS file.</summary>
		/// <remarks>
		/// Bind the JVM JAS setting to the specified JAAS file.
		/// <b>Important:</b> once a file has been loaded the JVM doesn't pick up
		/// changes
		/// </remarks>
		/// <param name="jaasFile">the JAAS file</param>
		public static void BindJVMtoJAASFile(FilePath jaasFile)
		{
			string path = jaasFile.GetAbsolutePath();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Binding {} to {}", Environment.JaasConfKey, path);
			}
			Runtime.SetProperty(Environment.JaasConfKey, path);
		}

		/// <summary>
		/// Set the Zookeeper server property
		/// <see cref="ZookeeperConfigOptions.PropZkServerSaslContext"/>
		/// to the SASL context. When the ZK server starts, this is the context
		/// which it will read in
		/// </summary>
		/// <param name="contextName">the name of the context</param>
		public static void BindZKToServerJAASContext(string contextName)
		{
			Runtime.SetProperty(PropZkServerSaslContext, contextName);
		}

		/// <summary>Reset any system properties related to JAAS</summary>
		public static void ClearJaasSystemProperties()
		{
			Runtime.ClearProperty(Environment.JaasConfKey);
		}

		/// <summary>Resolve the context of an entry.</summary>
		/// <remarks>
		/// Resolve the context of an entry. This is an effective test of
		/// JAAS setup, because it will relay detected problems up
		/// </remarks>
		/// <param name="context">context name</param>
		/// <returns>the entry</returns>
		/// <exception cref="Sharpen.RuntimeException">if there is no context entry found</exception>
		public static AppConfigurationEntry[] ValidateContext(string context)
		{
			if (context == null)
			{
				throw new RuntimeException("Null context argument");
			}
			if (context.IsEmpty())
			{
				throw new RuntimeException("Empty context argument");
			}
			Configuration configuration = Configuration.GetConfiguration();
			AppConfigurationEntry[] entries = configuration.GetAppConfigurationEntry(context);
			if (entries == null)
			{
				throw new RuntimeException(string.Format("Entry \"%s\" not found; " + "JAAS config = %s"
					, context, DescribeProperty(Environment.JaasConfKey)));
			}
			return entries;
		}

		/// <summary>Apply the security environment to this curator instance.</summary>
		/// <remarks>
		/// Apply the security environment to this curator instance. This
		/// may include setting up the ZK system properties for SASL
		/// </remarks>
		/// <param name="builder">curator builder</param>
		public virtual void ApplySecurityEnvironment(CuratorFrameworkFactory.Builder builder
			)
		{
			if (IsSecureRegistry())
			{
				switch (access)
				{
					case RegistrySecurity.AccessPolicy.anon:
					{
						ClearZKSaslClientProperties();
						break;
					}

					case RegistrySecurity.AccessPolicy.digest:
					{
						// no SASL
						ClearZKSaslClientProperties();
						builder.Authorization(SchemeDigest, digestAuthData);
						break;
					}

					case RegistrySecurity.AccessPolicy.sasl:
					{
						// bind to the current identity and context within the JAAS file
						SetZKSaslClientProperties(jaasClientIdentity, jaasClientContext);
						break;
					}
				}
			}
		}

		/// <summary>Set the client properties.</summary>
		/// <remarks>
		/// Set the client properties. This forces the ZK client into
		/// failing if it can't auth.
		/// <b>Important:</b>This is JVM-wide.
		/// </remarks>
		/// <param name="username">username</param>
		/// <param name="context">login context</param>
		/// <exception cref="Sharpen.RuntimeException">
		/// if the context cannot be found in the current
		/// JAAS context
		/// </exception>
		public static void SetZKSaslClientProperties(string username, string context)
		{
			Org.Apache.Hadoop.Registry.Client.Impl.ZK.RegistrySecurity.ValidateContext(context
				);
			EnableZookeeperClientSASL();
			Runtime.SetProperty(PropZkSaslClientUsername, username);
			Runtime.SetProperty(PropZkSaslClientContext, context);
		}

		/// <summary>
		/// Clear all the ZK SASL Client properties
		/// <b>Important:</b>This is JVM-wide
		/// </summary>
		public static void ClearZKSaslClientProperties()
		{
			DisableZookeeperClientSASL();
			Runtime.ClearProperty(PropZkSaslClientContext);
			Runtime.ClearProperty(PropZkSaslClientUsername);
		}

		/// <summary>
		/// Turn ZK SASL on
		/// <b>Important:</b>This is JVM-wide
		/// </summary>
		protected internal static void EnableZookeeperClientSASL()
		{
			Runtime.SetProperty(PropZkEnableSaslClient, "true");
		}

		/// <summary>Force disable ZK SASL bindings.</summary>
		/// <remarks>
		/// Force disable ZK SASL bindings.
		/// <b>Important:</b>This is JVM-wide
		/// </remarks>
		public static void DisableZookeeperClientSASL()
		{
			Runtime.SetProperty(ZookeeperConfigOptions.PropZkEnableSaslClient, "false");
		}

		/// <summary>Is the system property enabling the SASL client set?</summary>
		/// <returns>true if the SASL client system property is set.</returns>
		public static bool IsClientSASLEnabled()
		{
			return Sharpen.Extensions.ValueOf(Runtime.GetProperty(ZookeeperConfigOptions.PropZkEnableSaslClient
				, "true"));
		}

		/// <summary>Log details about the current Hadoop user at INFO.</summary>
		/// <remarks>
		/// Log details about the current Hadoop user at INFO.
		/// Robust against IOEs when trying to get the current user
		/// </remarks>
		public virtual void LogCurrentHadoopUser()
		{
			try
			{
				UserGroupInformation currentUser = UserGroupInformation.GetCurrentUser();
				Log.Info("Current user = {}", currentUser);
				UserGroupInformation realUser = currentUser.GetRealUser();
				Log.Info("Real User = {}", realUser);
			}
			catch (IOException e)
			{
				Log.Warn("Failed to get current user {}, {}", e);
			}
		}

		/// <summary>Stringify a list of ACLs for logging.</summary>
		/// <remarks>
		/// Stringify a list of ACLs for logging. Digest ACLs have their
		/// digest values stripped for security.
		/// </remarks>
		/// <param name="acls">ACL list</param>
		/// <returns>a string for logs, exceptions, ...</returns>
		public static string AclsToString(IList<ACL> acls)
		{
			StringBuilder builder = new StringBuilder();
			if (acls == null)
			{
				builder.Append("null ACL");
			}
			else
			{
				builder.Append('\n');
				foreach (ACL acl in acls)
				{
					builder.Append(AclToString(acl)).Append(" ");
				}
			}
			return builder.ToString();
		}

		/// <summary>Convert an ACL to a string, with any obfuscation needed</summary>
		/// <param name="acl">ACL</param>
		/// <returns>ACL string value</returns>
		public static string AclToString(ACL acl)
		{
			return string.Format(Sharpen.Extensions.GetEnglishCulture(), "0x%02x: %s", acl.GetPerms
				(), IdToString(acl.GetId()));
		}

		/// <summary>
		/// Convert an ID to a string, stripping out all but the first few characters
		/// of any digest auth hash for security reasons
		/// </summary>
		/// <param name="id">ID</param>
		/// <returns>a string description of a Zookeeper ID</returns>
		public static string IdToString(ID id)
		{
			string s;
			if (id.GetScheme().Equals(SchemeDigest))
			{
				string ids = id.GetId();
				int colon = ids.IndexOf(':');
				if (colon > 0)
				{
					ids = Sharpen.Runtime.Substring(ids, colon + 3);
				}
				s = SchemeDigest + ": " + ids;
			}
			else
			{
				s = id.ToString();
			}
			return s;
		}

		/// <summary>Build up low-level security diagnostics to aid debugging</summary>
		/// <returns>a string to use in diagnostics</returns>
		public virtual string BuildSecurityDiagnostics()
		{
			StringBuilder builder = new StringBuilder();
			builder.Append(secureRegistry ? "secure registry; " : "insecure registry; ");
			builder.Append("Curator service access policy: ").Append(access);
			builder.Append("; System ACLs: ").Append(AclsToString(systemACLs));
			builder.Append("User: ").Append(RegistrySecurity.UgiInfo.FromCurrentUser());
			builder.Append("; Kerberos Realm: ").Append(kerberosRealm);
			builder.Append(DescribeProperty(Environment.JaasConfKey));
			string sasl = Runtime.GetProperty(PropZkEnableSaslClient, DefaultZkEnableSaslClient
				);
			bool saslEnabled = Sharpen.Extensions.ValueOf(sasl);
			builder.Append(DescribeProperty(PropZkEnableSaslClient, DefaultZkEnableSaslClient
				));
			if (saslEnabled)
			{
				builder.Append("; JAAS Client Identity").Append("=").Append(jaasClientIdentity).Append
					("; ");
				builder.Append(KeyRegistryClientJaasContext).Append("=").Append(jaasClientContext
					).Append("; ");
				builder.Append(DescribeProperty(PropZkSaslClientUsername));
				builder.Append(DescribeProperty(PropZkSaslClientContext));
			}
			builder.Append(DescribeProperty(PropZkAllowFailedSaslClients, "(undefined but defaults to true)"
				));
			builder.Append(DescribeProperty(PropZkServerMaintainConnectionDespiteSaslFailure)
				);
			return builder.ToString();
		}

		private static string DescribeProperty(string name)
		{
			return DescribeProperty(name, "(undefined)");
		}

		private static string DescribeProperty(string name, string def)
		{
			return "; " + name + "=" + Runtime.GetProperty(name, def);
		}

		/// <summary>
		/// Get the default kerberos realm —returning "" if there
		/// is no realm or other problem
		/// </summary>
		/// <returns>
		/// the default realm of the system if it
		/// could be determined
		/// </returns>
		public static string GetDefaultRealmInJVM()
		{
			try
			{
				return KerberosUtil.GetDefaultRealm();
			}
			catch (TypeLoadException)
			{
			}
			catch (MissingMethodException)
			{
			}
			catch (MemberAccessException)
			{
			}
			catch (TargetInvocationException)
			{
			}
			// JDK7
			// ignored
			// ignored
			// ignored
			// ignored
			return string.Empty;
		}

		/// <summary>Create an ACL For a user.</summary>
		/// <param name="ugi">User identity</param>
		/// <returns>
		/// the ACL For the specified user. Ifthe username doesn't end
		/// in "@" then the realm is added
		/// </returns>
		public virtual ACL CreateACLForUser(UserGroupInformation ugi, int perms)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Creating ACL For ", new RegistrySecurity.UgiInfo(ugi));
			}
			if (!secureRegistry)
			{
				return AllReadwriteAccess;
			}
			else
			{
				return CreateACLfromUsername(ugi.GetUserName(), perms);
			}
		}

		/// <summary>Given a user name (short or long), create a SASL ACL</summary>
		/// <param name="username">
		/// user name; if it doesn't contain an "@" symbol, the
		/// service's kerberos realm is added
		/// </param>
		/// <param name="perms">permissions</param>
		/// <returns>an ACL for the user</returns>
		public virtual ACL CreateACLfromUsername(string username, int perms)
		{
			if (!username.Contains("@"))
			{
				username = username + "@" + kerberosRealm;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Appending kerberos realm to make {}", username);
				}
			}
			return new ACL(perms, new ID(SchemeSasl, username));
		}

		/// <summary>On demand string-ifier for UGI with extra details</summary>
		public class UgiInfo
		{
			public static RegistrySecurity.UgiInfo FromCurrentUser()
			{
				try
				{
					return new RegistrySecurity.UgiInfo(UserGroupInformation.GetCurrentUser());
				}
				catch (IOException e)
				{
					Log.Info("Failed to get current user {}", e, e);
					return new RegistrySecurity.UgiInfo(null);
				}
			}

			private readonly UserGroupInformation ugi;

			public UgiInfo(UserGroupInformation ugi)
			{
				this.ugi = ugi;
			}

			public override string ToString()
			{
				if (ugi == null)
				{
					return "(null ugi)";
				}
				StringBuilder builder = new StringBuilder();
				builder.Append(ugi.GetUserName()).Append(": ");
				builder.Append(ugi.ToString());
				builder.Append(" hasKerberosCredentials=").Append(ugi.HasKerberosCredentials());
				builder.Append(" isFromKeytab=").Append(ugi.IsFromKeytab());
				builder.Append(" kerberos is enabled in Hadoop =").Append(UserGroupInformation.IsSecurityEnabled
					());
				return builder.ToString();
			}
		}

		/// <summary>on-demand stringifier for a list of ACLs</summary>
		public class AclListInfo
		{
			public readonly IList<ACL> acls;

			public AclListInfo(IList<ACL> acls)
			{
				this.acls = acls;
			}

			public override string ToString()
			{
				return AclsToString(acls);
			}
		}
	}
}
