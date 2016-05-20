using Sharpen;

namespace org.apache.hadoop.security.authorize
{
	/// <summary>
	/// An authorization manager which handles service-level authorization
	/// for incoming service requests.
	/// </summary>
	public class ServiceAuthorizationManager
	{
		internal const string BLOCKED = ".blocked";

		internal const string HOSTS = ".hosts";

		private const string HADOOP_POLICY_FILE = "hadoop-policy.xml";

		private volatile System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.security.authorize.AccessControlList
			[]> protocolToAcls = new java.util.IdentityHashMap<java.lang.Class, org.apache.hadoop.security.authorize.AccessControlList
			[]>();

		private volatile System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.util.MachineList
			[]> protocolToMachineLists = new java.util.IdentityHashMap<java.lang.Class, org.apache.hadoop.util.MachineList
			[]>();

		/// <summary>Configuration key for controlling service-level authorization for Hadoop.
		/// 	</summary>
		[System.ObsoleteAttribute(@"Useorg.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION instead."
			)]
		public const string SERVICE_AUTHORIZATION_CONFIG = "hadoop.security.authorization";

		public static readonly org.apache.commons.logging.Log AUDITLOG = org.apache.commons.logging.LogFactory
			.getLog("SecurityLogger." + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authorize.ServiceAuthorizationManager
			)).getName());

		private const string AUTHZ_SUCCESSFUL_FOR = "Authorization successful for ";

		private const string AUTHZ_FAILED_FOR = "Authorization failed for ";

		// For each class, first ACL in the array specifies the allowed entries
		// and second ACL specifies blocked entries.
		// For each class, first MachineList in the array specifies the allowed entries
		// and second MachineList specifies blocked entries.
		/// <summary>Authorize the user to access the protocol being used.</summary>
		/// <param name="user">user accessing the service</param>
		/// <param name="protocol">service being accessed</param>
		/// <param name="conf">configuration to use</param>
		/// <param name="addr">InetAddress of the client</param>
		/// <exception cref="AuthorizationException">on authorization failure</exception>
		/// <exception cref="org.apache.hadoop.security.authorize.AuthorizationException"/>
		public virtual void authorize(org.apache.hadoop.security.UserGroupInformation user
			, java.lang.Class protocol, org.apache.hadoop.conf.Configuration conf, java.net.InetAddress
			 addr)
		{
			org.apache.hadoop.security.authorize.AccessControlList[] acls = protocolToAcls[protocol
				];
			org.apache.hadoop.util.MachineList[] hosts = protocolToMachineLists[protocol];
			if (acls == null || hosts == null)
			{
				throw new org.apache.hadoop.security.authorize.AuthorizationException("Protocol "
					 + protocol + " is not known.");
			}
			// get client principal key to verify (if available)
			org.apache.hadoop.security.KerberosInfo krbInfo = org.apache.hadoop.security.SecurityUtil
				.getKerberosInfo(protocol, conf);
			string clientPrincipal = null;
			if (krbInfo != null)
			{
				string clientKey = krbInfo.clientPrincipal();
				if (clientKey != null && !clientKey.isEmpty())
				{
					try
					{
						clientPrincipal = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(conf
							.get(clientKey), addr);
					}
					catch (System.IO.IOException e)
					{
						throw (org.apache.hadoop.security.authorize.AuthorizationException)new org.apache.hadoop.security.authorize.AuthorizationException
							("Can't figure out Kerberos principal name for connection from " + addr + " for user="
							 + user + " protocol=" + protocol).initCause(e);
					}
				}
			}
			if ((clientPrincipal != null && !clientPrincipal.Equals(user.getUserName())) || acls
				.Length != 2 || !acls[0].isUserAllowed(user) || acls[1].isUserAllowed(user))
			{
				AUDITLOG.warn(AUTHZ_FAILED_FOR + user + " for protocol=" + protocol + ", expected client Kerberos principal is "
					 + clientPrincipal);
				throw new org.apache.hadoop.security.authorize.AuthorizationException("User " + user
					 + " is not authorized for protocol " + protocol + ", expected client Kerberos principal is "
					 + clientPrincipal);
			}
			if (addr != null)
			{
				string hostAddress = addr.getHostAddress();
				if (hosts.Length != 2 || !hosts[0].includes(hostAddress) || hosts[1].includes(hostAddress
					))
				{
					AUDITLOG.warn(AUTHZ_FAILED_FOR + " for protocol=" + protocol + " from host = " + 
						hostAddress);
					throw new org.apache.hadoop.security.authorize.AuthorizationException("Host " + hostAddress
						 + " is not authorized for protocol " + protocol);
				}
			}
			AUDITLOG.info(AUTHZ_SUCCESSFUL_FOR + user + " for protocol=" + protocol);
		}

		public virtual void refresh(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.security.authorize.PolicyProvider
			 provider)
		{
			// Get the system property 'hadoop.policy.file'
			string policyFile = Sharpen.Runtime.getProperty("hadoop.policy.file", HADOOP_POLICY_FILE
				);
			// Make a copy of the original config, and load the policy file
			org.apache.hadoop.conf.Configuration policyConf = new org.apache.hadoop.conf.Configuration
				(conf);
			policyConf.addResource(policyFile);
			refreshWithLoadedConfiguration(policyConf, provider);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual void refreshWithLoadedConfiguration(org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.security.authorize.PolicyProvider provider)
		{
			System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.security.authorize.AccessControlList
				[]> newAcls = new java.util.IdentityHashMap<java.lang.Class, org.apache.hadoop.security.authorize.AccessControlList
				[]>();
			System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.util.MachineList
				[]> newMachineLists = new java.util.IdentityHashMap<java.lang.Class, org.apache.hadoop.util.MachineList
				[]>();
			string defaultAcl = conf.get(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_DEFAULT_ACL
				, org.apache.hadoop.security.authorize.AccessControlList.WILDCARD_ACL_VALUE);
			string defaultBlockedAcl = conf.get(org.apache.hadoop.fs.CommonConfigurationKeys.
				HADOOP_SECURITY_SERVICE_AUTHORIZATION_DEFAULT_BLOCKED_ACL, string.Empty);
			string defaultServiceHostsKey = getHostKey(org.apache.hadoop.fs.CommonConfigurationKeys
				.HADOOP_SECURITY_SERVICE_AUTHORIZATION_DEFAULT_ACL);
			string defaultMachineList = conf.get(defaultServiceHostsKey, org.apache.hadoop.util.MachineList
				.WILDCARD_VALUE);
			string defaultBlockedMachineList = conf.get(defaultServiceHostsKey + BLOCKED, string.Empty
				);
			// Parse the config file
			org.apache.hadoop.security.authorize.Service[] services = provider.getServices();
			if (services != null)
			{
				foreach (org.apache.hadoop.security.authorize.Service service in services)
				{
					org.apache.hadoop.security.authorize.AccessControlList acl = new org.apache.hadoop.security.authorize.AccessControlList
						(conf.get(service.getServiceKey(), defaultAcl));
					org.apache.hadoop.security.authorize.AccessControlList blockedAcl = new org.apache.hadoop.security.authorize.AccessControlList
						(conf.get(service.getServiceKey() + BLOCKED, defaultBlockedAcl));
					newAcls[service.getProtocol()] = new org.apache.hadoop.security.authorize.AccessControlList
						[] { acl, blockedAcl };
					string serviceHostsKey = getHostKey(service.getServiceKey());
					org.apache.hadoop.util.MachineList machineList = new org.apache.hadoop.util.MachineList
						(conf.get(serviceHostsKey, defaultMachineList));
					org.apache.hadoop.util.MachineList blockedMachineList = new org.apache.hadoop.util.MachineList
						(conf.get(serviceHostsKey + BLOCKED, defaultBlockedMachineList));
					newMachineLists[service.getProtocol()] = new org.apache.hadoop.util.MachineList[]
						 { machineList, blockedMachineList };
				}
			}
			// Flip to the newly parsed permissions
			protocolToAcls = newAcls;
			protocolToMachineLists = newMachineLists;
		}

		private string getHostKey(string serviceKey)
		{
			int endIndex = serviceKey.LastIndexOf(".");
			if (endIndex != -1)
			{
				return Sharpen.Runtime.substring(serviceKey, 0, endIndex) + HOSTS;
			}
			return serviceKey;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual System.Collections.Generic.ICollection<java.lang.Class> getProtocolsWithAcls
			()
		{
			return protocolToAcls.Keys;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual org.apache.hadoop.security.authorize.AccessControlList getProtocolsAcls
			(java.lang.Class className)
		{
			return protocolToAcls[className][0];
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual org.apache.hadoop.security.authorize.AccessControlList getProtocolsBlockedAcls
			(java.lang.Class className)
		{
			return protocolToAcls[className][1];
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual System.Collections.Generic.ICollection<java.lang.Class> getProtocolsWithMachineLists
			()
		{
			return protocolToMachineLists.Keys;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual org.apache.hadoop.util.MachineList getProtocolsMachineList(java.lang.Class
			 className)
		{
			return protocolToMachineLists[className][0];
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual org.apache.hadoop.util.MachineList getProtocolsBlockedMachineList(
			java.lang.Class className)
		{
			return protocolToMachineLists[className][1];
		}
	}
}
