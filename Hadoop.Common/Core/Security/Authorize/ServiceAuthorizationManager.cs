using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authorize
{
	/// <summary>
	/// An authorization manager which handles service-level authorization
	/// for incoming service requests.
	/// </summary>
	public class ServiceAuthorizationManager
	{
		internal const string Blocked = ".blocked";

		internal const string Hosts = ".hosts";

		private const string HadoopPolicyFile = "hadoop-policy.xml";

		private volatile IDictionary<Type, AccessControlList[]> protocolToAcls = new IdentityHashMap
			<Type, AccessControlList[]>();

		private volatile IDictionary<Type, MachineList[]> protocolToMachineLists = new IdentityHashMap
			<Type, MachineList[]>();

		/// <summary>Configuration key for controlling service-level authorization for Hadoop.
		/// 	</summary>
		[System.ObsoleteAttribute(@"UseOrg.Apache.Hadoop.FS.CommonConfigurationKeysPublic.HadoopSecurityAuthorization instead."
			)]
		public const string ServiceAuthorizationConfig = "hadoop.security.authorization";

		public static readonly Log Auditlog = LogFactory.GetLog("SecurityLogger." + typeof(
			ServiceAuthorizationManager).FullName);

		private const string AuthzSuccessfulFor = "Authorization successful for ";

		private const string AuthzFailedFor = "Authorization failed for ";

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
		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		public virtual void Authorize(UserGroupInformation user, Type protocol, Configuration
			 conf, IPAddress addr)
		{
			AccessControlList[] acls = protocolToAcls[protocol];
			MachineList[] hosts = protocolToMachineLists[protocol];
			if (acls == null || hosts == null)
			{
				throw new AuthorizationException("Protocol " + protocol + " is not known.");
			}
			// get client principal key to verify (if available)
			KerberosInfo krbInfo = SecurityUtil.GetKerberosInfo(protocol, conf);
			string clientPrincipal = null;
			if (krbInfo != null)
			{
				string clientKey = krbInfo.ClientPrincipal();
				if (clientKey != null && !clientKey.IsEmpty())
				{
					try
					{
						clientPrincipal = SecurityUtil.GetServerPrincipal(conf.Get(clientKey), addr);
					}
					catch (IOException e)
					{
						throw (AuthorizationException)Sharpen.Extensions.InitCause(new AuthorizationException
							("Can't figure out Kerberos principal name for connection from " + addr + " for user="
							 + user + " protocol=" + protocol), e);
					}
				}
			}
			if ((clientPrincipal != null && !clientPrincipal.Equals(user.GetUserName())) || acls
				.Length != 2 || !acls[0].IsUserAllowed(user) || acls[1].IsUserAllowed(user))
			{
				Auditlog.Warn(AuthzFailedFor + user + " for protocol=" + protocol + ", expected client Kerberos principal is "
					 + clientPrincipal);
				throw new AuthorizationException("User " + user + " is not authorized for protocol "
					 + protocol + ", expected client Kerberos principal is " + clientPrincipal);
			}
			if (addr != null)
			{
				string hostAddress = addr.GetHostAddress();
				if (hosts.Length != 2 || !hosts[0].Includes(hostAddress) || hosts[1].Includes(hostAddress
					))
				{
					Auditlog.Warn(AuthzFailedFor + " for protocol=" + protocol + " from host = " + hostAddress
						);
					throw new AuthorizationException("Host " + hostAddress + " is not authorized for protocol "
						 + protocol);
				}
			}
			Auditlog.Info(AuthzSuccessfulFor + user + " for protocol=" + protocol);
		}

		public virtual void Refresh(Configuration conf, PolicyProvider provider)
		{
			// Get the system property 'hadoop.policy.file'
			string policyFile = Runtime.GetProperty("hadoop.policy.file", HadoopPolicyFile);
			// Make a copy of the original config, and load the policy file
			Configuration policyConf = new Configuration(conf);
			policyConf.AddResource(policyFile);
			RefreshWithLoadedConfiguration(policyConf, provider);
		}

		[InterfaceAudience.Private]
		public virtual void RefreshWithLoadedConfiguration(Configuration conf, PolicyProvider
			 provider)
		{
			IDictionary<Type, AccessControlList[]> newAcls = new IdentityHashMap<Type, AccessControlList
				[]>();
			IDictionary<Type, MachineList[]> newMachineLists = new IdentityHashMap<Type, MachineList
				[]>();
			string defaultAcl = conf.Get(CommonConfigurationKeys.HadoopSecurityServiceAuthorizationDefaultAcl
				, AccessControlList.WildcardAclValue);
			string defaultBlockedAcl = conf.Get(CommonConfigurationKeys.HadoopSecurityServiceAuthorizationDefaultBlockedAcl
				, string.Empty);
			string defaultServiceHostsKey = GetHostKey(CommonConfigurationKeys.HadoopSecurityServiceAuthorizationDefaultAcl
				);
			string defaultMachineList = conf.Get(defaultServiceHostsKey, MachineList.WildcardValue
				);
			string defaultBlockedMachineList = conf.Get(defaultServiceHostsKey + Blocked, string.Empty
				);
			// Parse the config file
			Service[] services = provider.GetServices();
			if (services != null)
			{
				foreach (Service service in services)
				{
					AccessControlList acl = new AccessControlList(conf.Get(service.GetServiceKey(), defaultAcl
						));
					AccessControlList blockedAcl = new AccessControlList(conf.Get(service.GetServiceKey
						() + Blocked, defaultBlockedAcl));
					newAcls[service.GetProtocol()] = new AccessControlList[] { acl, blockedAcl };
					string serviceHostsKey = GetHostKey(service.GetServiceKey());
					MachineList machineList = new MachineList(conf.Get(serviceHostsKey, defaultMachineList
						));
					MachineList blockedMachineList = new MachineList(conf.Get(serviceHostsKey + Blocked
						, defaultBlockedMachineList));
					newMachineLists[service.GetProtocol()] = new MachineList[] { machineList, blockedMachineList
						 };
				}
			}
			// Flip to the newly parsed permissions
			protocolToAcls = newAcls;
			protocolToMachineLists = newMachineLists;
		}

		private string GetHostKey(string serviceKey)
		{
			int endIndex = serviceKey.LastIndexOf(".");
			if (endIndex != -1)
			{
				return Sharpen.Runtime.Substring(serviceKey, 0, endIndex) + Hosts;
			}
			return serviceKey;
		}

		[VisibleForTesting]
		public virtual ICollection<Type> GetProtocolsWithAcls()
		{
			return protocolToAcls.Keys;
		}

		[VisibleForTesting]
		public virtual AccessControlList GetProtocolsAcls(Type className)
		{
			return protocolToAcls[className][0];
		}

		[VisibleForTesting]
		public virtual AccessControlList GetProtocolsBlockedAcls(Type className)
		{
			return protocolToAcls[className][1];
		}

		[VisibleForTesting]
		public virtual ICollection<Type> GetProtocolsWithMachineLists()
		{
			return protocolToMachineLists.Keys;
		}

		[VisibleForTesting]
		public virtual MachineList GetProtocolsMachineList(Type className)
		{
			return protocolToMachineLists[className][0];
		}

		[VisibleForTesting]
		public virtual MachineList GetProtocolsBlockedMachineList(Type className)
		{
			return protocolToMachineLists[className][1];
		}
	}
}
