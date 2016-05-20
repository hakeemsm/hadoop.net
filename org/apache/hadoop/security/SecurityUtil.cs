using Sharpen;

namespace org.apache.hadoop.security
{
	public class SecurityUtil
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.SecurityUtil
			)));

		public const string HOSTNAME_PATTERN = "_HOST";

		public const string FAILED_TO_GET_UGI_MSG_HEADER = "Failed to obtain user group information:";

		[com.google.common.annotations.VisibleForTesting]
		internal static bool useIpForTokenService;

		[com.google.common.annotations.VisibleForTesting]
		internal static org.apache.hadoop.security.SecurityUtil.HostResolver hostResolver;

		static SecurityUtil()
		{
			//this will need to be replaced someday when there is a suitable replacement
			// controls whether buildTokenService will use an ip or host/ip as given
			// by the user
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			bool useIp = conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP
				, org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT
				);
			setTokenServiceUseIp(useIp);
		}

		/// <summary>For use only by tests and initialization</summary>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[com.google.common.annotations.VisibleForTesting]
		public static void setTokenServiceUseIp(bool flag)
		{
			useIpForTokenService = flag;
			hostResolver = !useIpForTokenService ? new org.apache.hadoop.security.SecurityUtil.QualifiedHostResolver
				() : new org.apache.hadoop.security.SecurityUtil.StandardHostResolver();
		}

		/// <summary>TGS must have the server principal of the form "krbtgt/FOO@FOO".</summary>
		/// <param name="principal"/>
		/// <returns>true or false</returns>
		internal static bool isTGSPrincipal(javax.security.auth.kerberos.KerberosPrincipal
			 principal)
		{
			if (principal == null)
			{
				return false;
			}
			if (principal.getName().Equals("krbtgt/" + principal.getRealm() + "@" + principal
				.getRealm()))
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
		protected internal static bool isOriginalTGT(javax.security.auth.kerberos.KerberosTicket
			 ticket)
		{
			return isTGSPrincipal(ticket.getServer());
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
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static string getServerPrincipal(string principalConfig, string hostname)
		{
			string[] components = getComponents(principalConfig);
			if (components == null || components.Length != 3 || !components[1].Equals(HOSTNAME_PATTERN
				))
			{
				return principalConfig;
			}
			else
			{
				return replacePattern(components, hostname);
			}
		}

		/// <summary>Convert Kerberos principal name pattern to valid Kerberos principal names.
		/// 	</summary>
		/// <remarks>
		/// Convert Kerberos principal name pattern to valid Kerberos principal names.
		/// This method is similar to
		/// <see cref="getServerPrincipal(string, string)"/>
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
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static string getServerPrincipal(string principalConfig, java.net.InetAddress
			 addr)
		{
			string[] components = getComponents(principalConfig);
			if (components == null || components.Length != 3 || !components[1].Equals(HOSTNAME_PATTERN
				))
			{
				return principalConfig;
			}
			else
			{
				if (addr == null)
				{
					throw new System.IO.IOException("Can't replace " + HOSTNAME_PATTERN + " pattern since client address is null"
						);
				}
				return replacePattern(components, addr.getCanonicalHostName());
			}
		}

		private static string[] getComponents(string principalConfig)
		{
			if (principalConfig == null)
			{
				return null;
			}
			return principalConfig.split("[/@]");
		}

		/// <exception cref="System.IO.IOException"/>
		private static string replacePattern(string[] components, string hostname)
		{
			string fqdn = hostname;
			if (fqdn == null || fqdn.isEmpty() || fqdn.Equals("0.0.0.0"))
			{
				fqdn = getLocalHostName();
			}
			return components[0] + "/" + org.apache.hadoop.util.StringUtils.toLowerCase(fqdn)
				 + "@" + components[2];
		}

		/// <exception cref="java.net.UnknownHostException"/>
		internal static string getLocalHostName()
		{
			return java.net.InetAddress.getLocalHost().getCanonicalHostName();
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
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static void login(org.apache.hadoop.conf.Configuration conf, string keytabFileKey
			, string userNameKey)
		{
			login(conf, keytabFileKey, userNameKey, getLocalHostName());
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
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static void login(org.apache.hadoop.conf.Configuration conf, string keytabFileKey
			, string userNameKey, string hostname)
		{
			if (!org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled())
			{
				return;
			}
			string keytabFilename = conf.get(keytabFileKey);
			if (keytabFilename == null || keytabFilename.Length == 0)
			{
				throw new System.IO.IOException("Running in secure mode, but config doesn't have a keytab"
					);
			}
			string principalConfig = conf.get(userNameKey, Sharpen.Runtime.getProperty("user.name"
				));
			string principalName = org.apache.hadoop.security.SecurityUtil.getServerPrincipal
				(principalConfig, hostname);
			org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(principalName
				, keytabFilename);
		}

		/// <summary>create the service name for a Delegation token</summary>
		/// <param name="uri">of the service</param>
		/// <param name="defPort">is used if the uri lacks a port</param>
		/// <returns>the token service, or null if no authority</returns>
		/// <seealso cref="buildTokenService(java.net.InetSocketAddress)"/>
		public static string buildDTServiceName(java.net.URI uri, int defPort)
		{
			string authority = uri.getAuthority();
			if (authority == null)
			{
				return null;
			}
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.createSocketAddr
				(authority, defPort);
			return buildTokenService(addr).ToString();
		}

		/// <summary>Get the host name from the principal name of format <service>/host@realm.
		/// 	</summary>
		/// <param name="principalName">principal name of format as described above</param>
		/// <returns>host name if the the string conforms to the above format, else null</returns>
		public static string getHostFromPrincipal(string principalName)
		{
			return new org.apache.hadoop.security.HadoopKerberosName(principalName).getHostName
				();
		}

		private static java.util.ServiceLoader<org.apache.hadoop.security.SecurityInfo> securityInfoProviders
			 = java.util.ServiceLoader.load<org.apache.hadoop.security.SecurityInfo>();

		private static org.apache.hadoop.security.SecurityInfo[] testProviders = new org.apache.hadoop.security.SecurityInfo
			[0];

		/// <summary>Test setup method to register additional providers.</summary>
		/// <param name="providers">a list of high priority providers to use</param>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static void setSecurityInfoProviders(params org.apache.hadoop.security.SecurityInfo
			[] providers)
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
		public static org.apache.hadoop.security.KerberosInfo getKerberosInfo(java.lang.Class
			 protocol, org.apache.hadoop.conf.Configuration conf)
		{
			foreach (org.apache.hadoop.security.SecurityInfo provider in testProviders)
			{
				org.apache.hadoop.security.KerberosInfo result = provider.getKerberosInfo(protocol
					, conf);
				if (result != null)
				{
					return result;
				}
			}
			lock (securityInfoProviders)
			{
				foreach (org.apache.hadoop.security.SecurityInfo provider_1 in securityInfoProviders)
				{
					org.apache.hadoop.security.KerberosInfo result = provider_1.getKerberosInfo(protocol
						, conf);
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
		public static org.apache.hadoop.security.token.TokenInfo getTokenInfo(java.lang.Class
			 protocol, org.apache.hadoop.conf.Configuration conf)
		{
			foreach (org.apache.hadoop.security.SecurityInfo provider in testProviders)
			{
				org.apache.hadoop.security.token.TokenInfo result = provider.getTokenInfo(protocol
					, conf);
				if (result != null)
				{
					return result;
				}
			}
			lock (securityInfoProviders)
			{
				foreach (org.apache.hadoop.security.SecurityInfo provider_1 in securityInfoProviders)
				{
					org.apache.hadoop.security.token.TokenInfo result = provider_1.getTokenInfo(protocol
						, conf);
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
		public static java.net.InetSocketAddress getTokenServiceAddr<_T0>(org.apache.hadoop.security.token.Token
			<_T0> token)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier
		{
			return org.apache.hadoop.net.NetUtils.createSocketAddr(token.getService().ToString
				());
		}

		/// <summary>Set the given token's service to the format expected by the RPC client</summary>
		/// <param name="token">a delegation token</param>
		/// <param name="addr">the socket for the rpc connection</param>
		public static void setTokenService<_T0>(org.apache.hadoop.security.token.Token<_T0
			> token, java.net.InetSocketAddress addr)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier
		{
			org.apache.hadoop.io.Text service = buildTokenService(addr);
			if (token != null)
			{
				token.setService(service);
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Acquired token " + token);
				}
			}
			else
			{
				// Token#toString() prints service
				LOG.warn("Failed to get token for service " + service);
			}
		}

		/// <summary>Construct the service key for a token</summary>
		/// <param name="addr">InetSocketAddress of remote connection with a token</param>
		/// <returns>
		/// "ip:port" or "host:port" depending on the value of
		/// hadoop.security.token.service.use_ip
		/// </returns>
		public static org.apache.hadoop.io.Text buildTokenService(java.net.InetSocketAddress
			 addr)
		{
			string host = null;
			if (useIpForTokenService)
			{
				if (addr.isUnresolved())
				{
					// host has no ip address
					throw new System.ArgumentException(new java.net.UnknownHostException(addr.getHostName
						()));
				}
				host = addr.getAddress().getHostAddress();
			}
			else
			{
				host = org.apache.hadoop.util.StringUtils.toLowerCase(addr.getHostName());
			}
			return new org.apache.hadoop.io.Text(host + ":" + addr.getPort());
		}

		/// <summary>Construct the service key for a token</summary>
		/// <param name="uri">of remote connection with a token</param>
		/// <returns>
		/// "ip:port" or "host:port" depending on the value of
		/// hadoop.security.token.service.use_ip
		/// </returns>
		public static org.apache.hadoop.io.Text buildTokenService(java.net.URI uri)
		{
			return buildTokenService(org.apache.hadoop.net.NetUtils.createSocketAddr(uri.getAuthority
				()));
		}

		/// <summary>Perform the given action as the daemon's login user.</summary>
		/// <remarks>
		/// Perform the given action as the daemon's login user. If the login
		/// user cannot be determined, this will log a FATAL error and exit
		/// the whole JVM.
		/// </remarks>
		public static T doAsLoginUserOrFatal<T>(java.security.PrivilegedAction<T> action)
		{
			if (org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled())
			{
				org.apache.hadoop.security.UserGroupInformation ugi = null;
				try
				{
					ugi = org.apache.hadoop.security.UserGroupInformation.getLoginUser();
				}
				catch (System.IO.IOException e)
				{
					LOG.fatal("Exception while getting login user", e);
					Sharpen.Runtime.printStackTrace(e);
					java.lang.Runtime.getRuntime().exit(-1);
				}
				return ugi.doAs(action);
			}
			else
			{
				return action.run();
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
		public static T doAsLoginUser<T>(java.security.PrivilegedExceptionAction<T> action
			)
		{
			return doAsUser(org.apache.hadoop.security.UserGroupInformation.getLoginUser(), action
				);
		}

		/// <summary>Perform the given action as the daemon's current user.</summary>
		/// <remarks>
		/// Perform the given action as the daemon's current user. If an
		/// InterruptedException is thrown, it is converted to an IOException.
		/// </remarks>
		/// <param name="action">the action to perform</param>
		/// <returns>the result of the action</returns>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		public static T doAsCurrentUser<T>(java.security.PrivilegedExceptionAction<T> action
			)
		{
			return doAsUser(org.apache.hadoop.security.UserGroupInformation.getCurrentUser(), 
				action);
		}

		/// <exception cref="System.IO.IOException"/>
		private static T doAsUser<T>(org.apache.hadoop.security.UserGroupInformation ugi, 
			java.security.PrivilegedExceptionAction<T> action)
		{
			try
			{
				return ugi.doAs(action);
			}
			catch (System.Exception ie)
			{
				throw new System.IO.IOException(ie);
			}
		}

		/// <summary>
		/// Resolves a host subject to the security requirements determined by
		/// hadoop.security.token.service.use_ip.
		/// </summary>
		/// <param name="hostname">host or ip to resolve</param>
		/// <returns>a resolved host</returns>
		/// <exception cref="java.net.UnknownHostException">if the host doesn't exist</exception>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static java.net.InetAddress getByName(string hostname)
		{
			return hostResolver.getByName(hostname);
		}

		internal interface HostResolver
		{
			/// <exception cref="java.net.UnknownHostException"/>
			java.net.InetAddress getByName(string host);
		}

		/// <summary>Uses standard java host resolution</summary>
		internal class StandardHostResolver : org.apache.hadoop.security.SecurityUtil.HostResolver
		{
			/// <exception cref="java.net.UnknownHostException"/>
			public virtual java.net.InetAddress getByName(string host)
			{
				return java.net.InetAddress.getByName(host);
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
		protected internal class QualifiedHostResolver : org.apache.hadoop.security.SecurityUtil.HostResolver
		{
			private System.Collections.Generic.IList<string> searchDomains = sun.net.dns.ResolverConfiguration
				.open().searchlist();

			/// <summary>
			/// Create an InetAddress with a fully qualified hostname of the given
			/// hostname.
			/// </summary>
			/// <remarks>
			/// Create an InetAddress with a fully qualified hostname of the given
			/// hostname.  InetAddress does not qualify an incomplete hostname that
			/// is resolved via the domain search list.
			/// <see cref="java.net.InetAddress.getCanonicalHostName()"/>
			/// will fully qualify the
			/// hostname, but it always return the A record whereas the given hostname
			/// may be a CNAME.
			/// </remarks>
			/// <param name="host">a hostname or ip address</param>
			/// <returns>InetAddress with the fully qualified hostname or ip</returns>
			/// <exception cref="java.net.UnknownHostException">if host does not exist</exception>
			public virtual java.net.InetAddress getByName(string host)
			{
				java.net.InetAddress addr = null;
				if (sun.net.util.IPAddressUtil.isIPv4LiteralAddress(host))
				{
					// use ipv4 address as-is
					byte[] ip = sun.net.util.IPAddressUtil.textToNumericFormatV4(host);
					addr = java.net.InetAddress.getByAddress(host, ip);
				}
				else
				{
					if (sun.net.util.IPAddressUtil.isIPv6LiteralAddress(host))
					{
						// use ipv6 address as-is
						byte[] ip = sun.net.util.IPAddressUtil.textToNumericFormatV6(host);
						addr = java.net.InetAddress.getByAddress(host, ip);
					}
					else
					{
						if (host.EndsWith("."))
						{
							// a rooted host ends with a dot, ex. "host."
							// rooted hosts never use the search path, so only try an exact lookup
							addr = getByExactName(host);
						}
						else
						{
							if (host.contains("."))
							{
								// the host contains a dot (domain), ex. "host.domain"
								// try an exact host lookup, then fallback to search list
								addr = getByExactName(host);
								if (addr == null)
								{
									addr = getByNameWithSearch(host);
								}
							}
							else
							{
								// it's a simple host with no dots, ex. "host"
								// try the search list, then fallback to exact host
								java.net.InetAddress loopback = java.net.InetAddress.getByName(null);
								if (Sharpen.Runtime.equalsIgnoreCase(host, loopback.getHostName()))
								{
									addr = java.net.InetAddress.getByAddress(host, loopback.getAddress());
								}
								else
								{
									addr = getByNameWithSearch(host);
									if (addr == null)
									{
										addr = getByExactName(host);
									}
								}
							}
						}
					}
				}
				// unresolvable!
				if (addr == null)
				{
					throw new java.net.UnknownHostException(host);
				}
				return addr;
			}

			internal virtual java.net.InetAddress getByExactName(string host)
			{
				java.net.InetAddress addr = null;
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
					addr = getInetAddressByName(fqHost);
					// can't leave the hostname as rooted or other parts of the system
					// malfunction, ex. kerberos principals are lacking proper host
					// equivalence for rooted/non-rooted hostnames
					addr = java.net.InetAddress.getByAddress(host, addr.getAddress());
				}
				catch (java.net.UnknownHostException)
				{
				}
				// ignore, caller will throw if necessary
				return addr;
			}

			internal virtual java.net.InetAddress getByNameWithSearch(string host)
			{
				java.net.InetAddress addr = null;
				if (host.EndsWith("."))
				{
					// already qualified?
					addr = getByExactName(host);
				}
				else
				{
					foreach (string domain in searchDomains)
					{
						string dot = !domain.StartsWith(".") ? "." : string.Empty;
						addr = getByExactName(host + dot + domain);
						if (addr != null)
						{
							break;
						}
					}
				}
				return addr;
			}

			// implemented as a separate method to facilitate unit testing
			/// <exception cref="java.net.UnknownHostException"/>
			internal virtual java.net.InetAddress getInetAddressByName(string host)
			{
				return java.net.InetAddress.getByName(host);
			}

			internal virtual void setSearchDomains(params string[] domains)
			{
				searchDomains = java.util.Arrays.asList(domains);
			}
		}

		public static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 getAuthenticationMethod(org.apache.hadoop.conf.Configuration conf)
		{
			string value = conf.get(org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION
				, "simple");
			try
			{
				return java.lang.Enum.valueOf<org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
					>(org.apache.hadoop.util.StringUtils.toUpperCase(value));
			}
			catch (System.ArgumentException)
			{
				throw new System.ArgumentException("Invalid attribute value for " + org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.HADOOP_SECURITY_AUTHENTICATION + " of " + value);
			}
		}

		public static void setAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 authenticationMethod, org.apache.hadoop.conf.Configuration conf)
		{
			if (authenticationMethod == null)
			{
				authenticationMethod = org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
					.SIMPLE;
			}
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION
				, org.apache.hadoop.util.StringUtils.toLowerCase(authenticationMethod.ToString()
				));
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
		public static bool isPrivilegedPort(int port)
		{
			return port < 1024;
		}
	}
}
