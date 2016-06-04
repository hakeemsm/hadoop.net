using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Javax.Security.Auth.Kerberos;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sun.Net.Dns;
using Sun.Net.Util;

namespace Org.Apache.Hadoop.Security
{
	public class SecurityUtil
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(SecurityUtil));

		public const string HostnamePattern = "_HOST";

		public const string FailedToGetUgiMsgHeader = "Failed to obtain user group information:";

		[VisibleForTesting]
		internal static bool useIpForTokenService;

		[VisibleForTesting]
		internal static SecurityUtil.HostResolver hostResolver;

		static SecurityUtil()
		{
			//this will need to be replaced someday when there is a suitable replacement
			// controls whether buildTokenService will use an ip or host/ip as given
			// by the user
			Configuration conf = new Configuration();
			bool useIp = conf.GetBoolean(CommonConfigurationKeys.HadoopSecurityTokenServiceUseIp
				, CommonConfigurationKeys.HadoopSecurityTokenServiceUseIpDefault);
			SetTokenServiceUseIp(useIp);
		}

		/// <summary>For use only by tests and initialization</summary>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		public static void SetTokenServiceUseIp(bool flag)
		{
			useIpForTokenService = flag;
			hostResolver = !useIpForTokenService ? new SecurityUtil.QualifiedHostResolver() : 
				new SecurityUtil.StandardHostResolver();
		}

		/// <summary>TGS must have the server principal of the form "krbtgt/FOO@FOO".</summary>
		/// <param name="principal"/>
		/// <returns>true or false</returns>
		internal static bool IsTGSPrincipal(KerberosPrincipal principal)
		{
			if (principal == null)
			{
				return false;
			}
			if (principal.GetName().Equals("krbtgt/" + principal.GetRealm() + "@" + principal
				.GetRealm()))
			{
				return true;
			}
			return false;
		}

		/// <summary>Check whether the server principal is the TGS's principal</summary>
		/// <param name="ticket">
		/// the original TGT (the ticket that is obtained when a
		/// kinit is done)
		/// </param>
		/// <returns>true or false</returns>
		protected internal static bool IsOriginalTGT(KerberosTicket ticket)
		{
			return IsTGSPrincipal(ticket.GetServer());
		}

		/// <summary>
		/// Convert Kerberos principal name pattern to valid Kerberos principal
		/// names.
		/// </summary>
		/// <remarks>
		/// Convert Kerberos principal name pattern to valid Kerberos principal
		/// names. It replaces hostname pattern with hostname, which should be
		/// fully-qualified domain name. If hostname is null or "0.0.0.0", it uses
		/// dynamically looked-up fqdn of the current host instead.
		/// </remarks>
		/// <param name="principalConfig">the Kerberos principal name conf value to convert</param>
		/// <param name="hostname">the fully-qualified domain name used for substitution</param>
		/// <returns>converted Kerberos principal name</returns>
		/// <exception cref="System.IO.IOException">if the client address cannot be determined
		/// 	</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static string GetServerPrincipal(string principalConfig, string hostname)
		{
			string[] components = GetComponents(principalConfig);
			if (components == null || components.Length != 3 || !components[1].Equals(HostnamePattern
				))
			{
				return principalConfig;
			}
			else
			{
				return ReplacePattern(components, hostname);
			}
		}

		/// <summary>Convert Kerberos principal name pattern to valid Kerberos principal names.
		/// 	</summary>
		/// <remarks>
		/// Convert Kerberos principal name pattern to valid Kerberos principal names.
		/// This method is similar to
		/// <see cref="GetServerPrincipal(string, string)"/>
		/// ,
		/// except 1) the reverse DNS lookup from addr to hostname is done only when
		/// necessary, 2) param addr can't be null (no default behavior of using local
		/// hostname when addr is null).
		/// </remarks>
		/// <param name="principalConfig">Kerberos principal name pattern to convert</param>
		/// <param name="addr">InetAddress of the host used for substitution</param>
		/// <returns>converted Kerberos principal name</returns>
		/// <exception cref="System.IO.IOException">if the client address cannot be determined
		/// 	</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static string GetServerPrincipal(string principalConfig, IPAddress addr)
		{
			string[] components = GetComponents(principalConfig);
			if (components == null || components.Length != 3 || !components[1].Equals(HostnamePattern
				))
			{
				return principalConfig;
			}
			else
			{
				if (addr == null)
				{
					throw new IOException("Can't replace " + HostnamePattern + " pattern since client address is null"
						);
				}
				return ReplacePattern(components, addr.ToString());
			}
		}

		private static string[] GetComponents(string principalConfig)
		{
			if (principalConfig == null)
			{
				return null;
			}
			return principalConfig.Split("[/@]");
		}

		/// <exception cref="System.IO.IOException"/>
		private static string ReplacePattern(string[] components, string hostname)
		{
			string fqdn = hostname;
			if (fqdn == null || fqdn.IsEmpty() || fqdn.Equals("0.0.0.0"))
			{
				fqdn = GetLocalHostName();
			}
			return components[0] + "/" + StringUtils.ToLowerCase(fqdn) + "@" + components[2];
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		internal static string GetLocalHostName()
		{
			return Sharpen.Runtime.GetLocalHost().ToString();
		}

		/// <summary>Login as a principal specified in config.</summary>
		/// <remarks>
		/// Login as a principal specified in config. Substitute $host in
		/// user's Kerberos principal name with a dynamically looked-up fully-qualified
		/// domain name of the current host.
		/// </remarks>
		/// <param name="conf">conf to use</param>
		/// <param name="keytabFileKey">the key to look for keytab file in conf</param>
		/// <param name="userNameKey">the key to look for user's Kerberos principal name in conf
		/// 	</param>
		/// <exception cref="System.IO.IOException">if login fails</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static void Login(Configuration conf, string keytabFileKey, string userNameKey
			)
		{
			Login(conf, keytabFileKey, userNameKey, GetLocalHostName());
		}

		/// <summary>Login as a principal specified in config.</summary>
		/// <remarks>
		/// Login as a principal specified in config. Substitute $host in user's Kerberos principal
		/// name with hostname. If non-secure mode - return. If no keytab available -
		/// bail out with an exception
		/// </remarks>
		/// <param name="conf">conf to use</param>
		/// <param name="keytabFileKey">the key to look for keytab file in conf</param>
		/// <param name="userNameKey">the key to look for user's Kerberos principal name in conf
		/// 	</param>
		/// <param name="hostname">hostname to use for substitution</param>
		/// <exception cref="System.IO.IOException">if the config doesn't specify a keytab</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static void Login(Configuration conf, string keytabFileKey, string userNameKey
			, string hostname)
		{
			if (!UserGroupInformation.IsSecurityEnabled())
			{
				return;
			}
			string keytabFilename = conf.Get(keytabFileKey);
			if (keytabFilename == null || keytabFilename.Length == 0)
			{
				throw new IOException("Running in secure mode, but config doesn't have a keytab");
			}
			string principalConfig = conf.Get(userNameKey, Runtime.GetProperty("user.name"));
			string principalName = SecurityUtil.GetServerPrincipal(principalConfig, hostname);
			UserGroupInformation.LoginUserFromKeytab(principalName, keytabFilename);
		}

		/// <summary>create the service name for a Delegation token</summary>
		/// <param name="uri">of the service</param>
		/// <param name="defPort">is used if the uri lacks a port</param>
		/// <returns>the token service, or null if no authority</returns>
		/// <seealso cref="BuildTokenService(System.Net.IPEndPoint)"/>
		public static string BuildDTServiceName(URI uri, int defPort)
		{
			string authority = uri.GetAuthority();
			if (authority == null)
			{
				return null;
			}
			IPEndPoint addr = NetUtils.CreateSocketAddr(authority, defPort);
			return BuildTokenService(addr).ToString();
		}

		/// <summary>Get the host name from the principal name of format <service>/host@realm.
		/// 	</summary>
		/// <param name="principalName">principal name of format as described above</param>
		/// <returns>host name if the the string conforms to the above format, else null</returns>
		public static string GetHostFromPrincipal(string principalName)
		{
			return new HadoopKerberosName(principalName).GetHostName();
		}

		private static ServiceLoader<SecurityInfo> securityInfoProviders = ServiceLoader.
			Load<SecurityInfo>();

		private static SecurityInfo[] testProviders = new SecurityInfo[0];

		/// <summary>Test setup method to register additional providers.</summary>
		/// <param name="providers">a list of high priority providers to use</param>
		[InterfaceAudience.Private]
		public static void SetSecurityInfoProviders(params SecurityInfo[] providers)
		{
			testProviders = providers;
		}

		/// <summary>Look up the KerberosInfo for a given protocol.</summary>
		/// <remarks>
		/// Look up the KerberosInfo for a given protocol. It searches all known
		/// SecurityInfo providers.
		/// </remarks>
		/// <param name="protocol">the protocol class to get the information for</param>
		/// <param name="conf">configuration object</param>
		/// <returns>the KerberosInfo or null if it has no KerberosInfo defined</returns>
		public static KerberosInfo GetKerberosInfo(Type protocol, Configuration conf)
		{
			foreach (SecurityInfo provider in testProviders)
			{
				KerberosInfo result = provider.GetKerberosInfo(protocol, conf);
				if (result != null)
				{
					return result;
				}
			}
			lock (securityInfoProviders)
			{
				foreach (SecurityInfo provider_1 in securityInfoProviders)
				{
					KerberosInfo result = provider_1.GetKerberosInfo(protocol, conf);
					if (result != null)
					{
						return result;
					}
				}
			}
			return null;
		}

		/// <summary>Look up the TokenInfo for a given protocol.</summary>
		/// <remarks>
		/// Look up the TokenInfo for a given protocol. It searches all known
		/// SecurityInfo providers.
		/// </remarks>
		/// <param name="protocol">The protocol class to get the information for.</param>
		/// <param name="conf">Configuration object</param>
		/// <returns>the TokenInfo or null if it has no KerberosInfo defined</returns>
		public static TokenInfo GetTokenInfo(Type protocol, Configuration conf)
		{
			foreach (SecurityInfo provider in testProviders)
			{
				TokenInfo result = provider.GetTokenInfo(protocol, conf);
				if (result != null)
				{
					return result;
				}
			}
			lock (securityInfoProviders)
			{
				foreach (SecurityInfo provider_1 in securityInfoProviders)
				{
					TokenInfo result = provider_1.GetTokenInfo(protocol, conf);
					if (result != null)
					{
						return result;
					}
				}
			}
			return null;
		}

		/// <summary>Decode the given token's service field into an InetAddress</summary>
		/// <param name="token">from which to obtain the service</param>
		/// <returns>InetAddress for the service</returns>
		public static IPEndPoint GetTokenServiceAddr<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			return NetUtils.CreateSocketAddr(token.GetService().ToString());
		}

		/// <summary>Set the given token's service to the format expected by the RPC client</summary>
		/// <param name="token">a delegation token</param>
		/// <param name="addr">the socket for the rpc connection</param>
		public static void SetTokenService<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0
			> token, IPEndPoint addr)
			where _T0 : TokenIdentifier
		{
			Text service = BuildTokenService(addr);
			if (token != null)
			{
				token.SetService(service);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Acquired token " + token);
				}
			}
			else
			{
				// Token#toString() prints service
				Log.Warn("Failed to get token for service " + service);
			}
		}

		/// <summary>Construct the service key for a token</summary>
		/// <param name="addr">InetSocketAddress of remote connection with a token</param>
		/// <returns>
		/// "ip:port" or "host:port" depending on the value of
		/// hadoop.security.token.service.use_ip
		/// </returns>
		public static Text BuildTokenService(IPEndPoint addr)
		{
			string host = null;
			if (useIpForTokenService)
			{
				if (addr.IsUnresolved())
				{
					// host has no ip address
					throw new ArgumentException(new UnknownHostException(addr.GetHostName()));
				}
				host = addr.Address.GetHostAddress();
			}
			else
			{
				host = StringUtils.ToLowerCase(addr.GetHostName());
			}
			return new Text(host + ":" + addr.Port);
		}

		/// <summary>Construct the service key for a token</summary>
		/// <param name="uri">of remote connection with a token</param>
		/// <returns>
		/// "ip:port" or "host:port" depending on the value of
		/// hadoop.security.token.service.use_ip
		/// </returns>
		public static Text BuildTokenService(URI uri)
		{
			return BuildTokenService(NetUtils.CreateSocketAddr(uri.GetAuthority()));
		}

		/// <summary>Perform the given action as the daemon's login user.</summary>
		/// <remarks>
		/// Perform the given action as the daemon's login user. If the login
		/// user cannot be determined, this will log a FATAL error and exit
		/// the whole JVM.
		/// </remarks>
		public static T DoAsLoginUserOrFatal<T>(PrivilegedAction<T> action)
		{
			if (UserGroupInformation.IsSecurityEnabled())
			{
				UserGroupInformation ugi = null;
				try
				{
					ugi = UserGroupInformation.GetLoginUser();
				}
				catch (IOException e)
				{
					Log.Fatal("Exception while getting login user", e);
					Sharpen.Runtime.PrintStackTrace(e);
					Runtime.GetRuntime().Exit(-1);
				}
				return ugi.DoAs(action);
			}
			else
			{
				return action.Run();
			}
		}

		/// <summary>Perform the given action as the daemon's login user.</summary>
		/// <remarks>
		/// Perform the given action as the daemon's login user. If an
		/// InterruptedException is thrown, it is converted to an IOException.
		/// </remarks>
		/// <param name="action">the action to perform</param>
		/// <returns>the result of the action</returns>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		public static T DoAsLoginUser<T>(PrivilegedExceptionAction<T> action)
		{
			return DoAsUser(UserGroupInformation.GetLoginUser(), action);
		}

		/// <summary>Perform the given action as the daemon's current user.</summary>
		/// <remarks>
		/// Perform the given action as the daemon's current user. If an
		/// InterruptedException is thrown, it is converted to an IOException.
		/// </remarks>
		/// <param name="action">the action to perform</param>
		/// <returns>the result of the action</returns>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		public static T DoAsCurrentUser<T>(PrivilegedExceptionAction<T> action)
		{
			return DoAsUser(UserGroupInformation.GetCurrentUser(), action);
		}

		/// <exception cref="System.IO.IOException"/>
		private static T DoAsUser<T>(UserGroupInformation ugi, PrivilegedExceptionAction<
			T> action)
		{
			try
			{
				return ugi.DoAs(action);
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		/// <summary>
		/// Resolves a host subject to the security requirements determined by
		/// hadoop.security.token.service.use_ip.
		/// </summary>
		/// <param name="hostname">host or ip to resolve</param>
		/// <returns>a resolved host</returns>
		/// <exception cref="Sharpen.UnknownHostException">if the host doesn't exist</exception>
		[InterfaceAudience.Private]
		public static IPAddress GetByName(string hostname)
		{
			return hostResolver.GetByName(hostname);
		}

		internal interface HostResolver
		{
			/// <exception cref="Sharpen.UnknownHostException"/>
			IPAddress GetByName(string host);
		}

		/// <summary>Uses standard java host resolution</summary>
		internal class StandardHostResolver : SecurityUtil.HostResolver
		{
			/// <exception cref="Sharpen.UnknownHostException"/>
			public virtual IPAddress GetByName(string host)
			{
				return Sharpen.Extensions.GetAddressByName(host);
			}
		}

		/// <summary>
		/// This an alternate resolver with important properties that the standard
		/// java resolver lacks:
		/// 1) The hostname is fully qualified.
		/// </summary>
		/// <remarks>
		/// This an alternate resolver with important properties that the standard
		/// java resolver lacks:
		/// 1) The hostname is fully qualified.  This avoids security issues if not
		/// all hosts in the cluster do not share the same search domains.  It
		/// also prevents other hosts from performing unnecessary dns searches.
		/// In contrast, InetAddress simply returns the host as given.
		/// 2) The InetAddress is instantiated with an exact host and IP to prevent
		/// further unnecessary lookups.  InetAddress may perform an unnecessary
		/// reverse lookup for an IP.
		/// 3) A call to getHostName() will always return the qualified hostname, or
		/// more importantly, the IP if instantiated with an IP.  This avoids
		/// unnecessary dns timeouts if the host is not resolvable.
		/// 4) Point 3 also ensures that if the host is re-resolved, ex. during a
		/// connection re-attempt, that a reverse lookup to host and forward
		/// lookup to IP is not performed since the reverse/forward mappings may
		/// not always return the same IP.  If the client initiated a connection
		/// with an IP, then that IP is all that should ever be contacted.
		/// NOTE: this resolver is only used if:
		/// hadoop.security.token.service.use_ip=false
		/// </remarks>
		protected internal class QualifiedHostResolver : SecurityUtil.HostResolver
		{
			private IList<string> searchDomains = ResolverConfiguration.Open().Searchlist();

			/// <summary>
			/// Create an InetAddress with a fully qualified hostname of the given
			/// hostname.
			/// </summary>
			/// <remarks>
			/// Create an InetAddress with a fully qualified hostname of the given
			/// hostname.  InetAddress does not qualify an incomplete hostname that
			/// is resolved via the domain search list.
			/// <see cref="System.Net.IPAddress.ToString()"/>
			/// will fully qualify the
			/// hostname, but it always return the A record whereas the given hostname
			/// may be a CNAME.
			/// </remarks>
			/// <param name="host">a hostname or ip address</param>
			/// <returns>InetAddress with the fully qualified hostname or ip</returns>
			/// <exception cref="Sharpen.UnknownHostException">if host does not exist</exception>
			public virtual IPAddress GetByName(string host)
			{
				IPAddress addr = null;
				if (IPAddressUtil.IsIPv4LiteralAddress(host))
				{
					// use ipv4 address as-is
					byte[] ip = IPAddressUtil.TextToNumericFormatV4(host);
					addr = IPAddress.GetByAddress(host, ip);
				}
				else
				{
					if (IPAddressUtil.IsIPv6LiteralAddress(host))
					{
						// use ipv6 address as-is
						byte[] ip = IPAddressUtil.TextToNumericFormatV6(host);
						addr = IPAddress.GetByAddress(host, ip);
					}
					else
					{
						if (host.EndsWith("."))
						{
							// a rooted host ends with a dot, ex. "host."
							// rooted hosts never use the search path, so only try an exact lookup
							addr = GetByExactName(host);
						}
						else
						{
							if (host.Contains("."))
							{
								// the host contains a dot (domain), ex. "host.domain"
								// try an exact host lookup, then fallback to search list
								addr = GetByExactName(host);
								if (addr == null)
								{
									addr = GetByNameWithSearch(host);
								}
							}
							else
							{
								// it's a simple host with no dots, ex. "host"
								// try the search list, then fallback to exact host
								IPAddress loopback = Sharpen.Extensions.GetAddressByName(null);
								if (Sharpen.Runtime.EqualsIgnoreCase(host, loopback.GetHostName()))
								{
									addr = IPAddress.GetByAddress(host, loopback.GetAddressBytes());
								}
								else
								{
									addr = GetByNameWithSearch(host);
									if (addr == null)
									{
										addr = GetByExactName(host);
									}
								}
							}
						}
					}
				}
				// unresolvable!
				if (addr == null)
				{
					throw new UnknownHostException(host);
				}
				return addr;
			}

			internal virtual IPAddress GetByExactName(string host)
			{
				IPAddress addr = null;
				// InetAddress will use the search list unless the host is rooted
				// with a trailing dot.  The trailing dot will disable any use of the
				// search path in a lower level resolver.  See RFC 1535.
				string fqHost = host;
				if (!fqHost.EndsWith("."))
				{
					fqHost += ".";
				}
				try
				{
					addr = GetInetAddressByName(fqHost);
					// can't leave the hostname as rooted or other parts of the system
					// malfunction, ex. kerberos principals are lacking proper host
					// equivalence for rooted/non-rooted hostnames
					addr = IPAddress.GetByAddress(host, addr.GetAddressBytes());
				}
				catch (UnknownHostException)
				{
				}
				// ignore, caller will throw if necessary
				return addr;
			}

			internal virtual IPAddress GetByNameWithSearch(string host)
			{
				IPAddress addr = null;
				if (host.EndsWith("."))
				{
					// already qualified?
					addr = GetByExactName(host);
				}
				else
				{
					foreach (string domain in searchDomains)
					{
						string dot = !domain.StartsWith(".") ? "." : string.Empty;
						addr = GetByExactName(host + dot + domain);
						if (addr != null)
						{
							break;
						}
					}
				}
				return addr;
			}

			// implemented as a separate method to facilitate unit testing
			/// <exception cref="Sharpen.UnknownHostException"/>
			internal virtual IPAddress GetInetAddressByName(string host)
			{
				return Sharpen.Extensions.GetAddressByName(host);
			}

			internal virtual void SetSearchDomains(params string[] domains)
			{
				searchDomains = Arrays.AsList(domains);
			}
		}

		public static UserGroupInformation.AuthenticationMethod GetAuthenticationMethod(Configuration
			 conf)
		{
			string value = conf.Get(CommonConfigurationKeysPublic.HadoopSecurityAuthentication
				, "simple");
			try
			{
				return Enum.ValueOf<UserGroupInformation.AuthenticationMethod>(StringUtils.ToUpperCase
					(value));
			}
			catch (ArgumentException)
			{
				throw new ArgumentException("Invalid attribute value for " + CommonConfigurationKeysPublic
					.HadoopSecurityAuthentication + " of " + value);
			}
		}

		public static void SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod
			 authenticationMethod, Configuration conf)
		{
			if (authenticationMethod == null)
			{
				authenticationMethod = UserGroupInformation.AuthenticationMethod.Simple;
			}
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, StringUtils.
				ToLowerCase(authenticationMethod.ToString()));
		}

		/*
		* Check if a given port is privileged.
		* The ports with number smaller than 1024 are treated as privileged ports in
		* unix/linux system. For other operating systems, use this method with care.
		* For example, Windows doesn't have the concept of privileged ports.
		* However, it may be used at Windows client to check port of linux server.
		*
		* @param port the port number
		* @return true for privileged ports, false otherwise
		*
		*/
		public static bool IsPrivilegedPort(int port)
		{
			return port < 1024;
		}
	}
}
