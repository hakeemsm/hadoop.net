using Sharpen;

namespace org.apache.hadoop.net
{
	public class NetUtils
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.net.NetUtils)));

		private static System.Collections.Generic.IDictionary<string, string> hostToResolved
			 = new System.Collections.Generic.Dictionary<string, string>();

		/// <summary>
		/// text to point users elsewhere:
		/// <value/>
		/// 
		/// </summary>
		private const string FOR_MORE_DETAILS_SEE = " For more details see:  ";

		/// <summary>
		/// text included in wrapped exceptions if the host is null:
		/// <value/>
		/// 
		/// </summary>
		public const string UNKNOWN_HOST = "(unknown)";

		/// <summary>
		/// Base URL of the Hadoop Wiki:
		/// <value/>
		/// 
		/// </summary>
		public const string HADOOP_WIKI = "http://wiki.apache.org/hadoop/";

		/// <summary>
		/// Get the socket factory for the given class according to its
		/// configuration parameter
		/// <tt>hadoop.rpc.socket.factory.class.&lt;ClassName&gt;</tt>.
		/// </summary>
		/// <remarks>
		/// Get the socket factory for the given class according to its
		/// configuration parameter
		/// <tt>hadoop.rpc.socket.factory.class.&lt;ClassName&gt;</tt>. When no
		/// such parameter exists then fall back on the default socket factory as
		/// configured by <tt>hadoop.rpc.socket.factory.class.default</tt>. If
		/// this default socket factory is not configured, then fall back on the JVM
		/// default socket factory.
		/// </remarks>
		/// <param name="conf">the configuration</param>
		/// <param name="clazz">
		/// the class (usually a
		/// <see cref="org.apache.hadoop.ipc.VersionedProtocol"/>
		/// )
		/// </param>
		/// <returns>a socket factory</returns>
		public static javax.net.SocketFactory getSocketFactory(org.apache.hadoop.conf.Configuration
			 conf, java.lang.Class clazz)
		{
			javax.net.SocketFactory factory = null;
			string propValue = conf.get("hadoop.rpc.socket.factory.class." + clazz.getSimpleName
				());
			if ((propValue != null) && (propValue.Length > 0))
			{
				factory = getSocketFactoryFromProperty(conf, propValue);
			}
			if (factory == null)
			{
				factory = getDefaultSocketFactory(conf);
			}
			return factory;
		}

		/// <summary>
		/// Get the default socket factory as specified by the configuration
		/// parameter <tt>hadoop.rpc.socket.factory.default</tt>
		/// </summary>
		/// <param name="conf">the configuration</param>
		/// <returns>
		/// the default socket factory as specified in the configuration or
		/// the JVM default socket factory if the configuration does not
		/// contain a default socket factory property.
		/// </returns>
		public static javax.net.SocketFactory getDefaultSocketFactory(org.apache.hadoop.conf.Configuration
			 conf)
		{
			string propValue = conf.get(org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT
				);
			if ((propValue == null) || (propValue.Length == 0))
			{
				return javax.net.SocketFactory.getDefault();
			}
			return getSocketFactoryFromProperty(conf, propValue);
		}

		/// <summary>Get the socket factory corresponding to the given proxy URI.</summary>
		/// <remarks>
		/// Get the socket factory corresponding to the given proxy URI. If the
		/// given proxy URI corresponds to an absence of configuration parameter,
		/// returns null. If the URI is malformed raises an exception.
		/// </remarks>
		/// <param name="propValue">
		/// the property which is the class name of the
		/// SocketFactory to instantiate; assumed non null and non empty.
		/// </param>
		/// <returns>a socket factory as defined in the property value.</returns>
		public static javax.net.SocketFactory getSocketFactoryFromProperty(org.apache.hadoop.conf.Configuration
			 conf, string propValue)
		{
			try
			{
				java.lang.Class theClass = conf.getClassByName(propValue);
				return (javax.net.SocketFactory)org.apache.hadoop.util.ReflectionUtils.newInstance
					(theClass, conf);
			}
			catch (java.lang.ClassNotFoundException cnfe)
			{
				throw new System.Exception("Socket Factory class not found: " + cnfe);
			}
		}

		/// <summary>
		/// Util method to build socket addr from either:
		/// <host>:<port>
		/// <fs>://<host>:<port>/<path>
		/// </summary>
		public static java.net.InetSocketAddress createSocketAddr(string target)
		{
			return createSocketAddr(target, -1);
		}

		/// <summary>
		/// Util method to build socket addr from either:
		/// <host>
		/// <host>:<port>
		/// <fs>://<host>:<port>/<path>
		/// </summary>
		public static java.net.InetSocketAddress createSocketAddr(string target, int defaultPort
			)
		{
			return createSocketAddr(target, defaultPort, null);
		}

		/// <summary>
		/// Create an InetSocketAddress from the given target string and
		/// default port.
		/// </summary>
		/// <remarks>
		/// Create an InetSocketAddress from the given target string and
		/// default port. If the string cannot be parsed correctly, the
		/// <code>configName</code> parameter is used as part of the
		/// exception message, allowing the user to better diagnose
		/// the misconfiguration.
		/// </remarks>
		/// <param name="target">a string of either "host" or "host:port"</param>
		/// <param name="defaultPort">
		/// the default port if <code>target</code> does not
		/// include a port number
		/// </param>
		/// <param name="configName">
		/// the name of the configuration from which
		/// <code>target</code> was loaded. This is used in the
		/// exception message in the case that parsing fails.
		/// </param>
		public static java.net.InetSocketAddress createSocketAddr(string target, int defaultPort
			, string configName)
		{
			string helpText = string.Empty;
			if (configName != null)
			{
				helpText = " (configuration property '" + configName + "')";
			}
			if (target == null)
			{
				throw new System.ArgumentException("Target address cannot be null." + helpText);
			}
			target = target.Trim();
			bool hasScheme = target.contains("://");
			java.net.URI uri = null;
			try
			{
				uri = hasScheme ? java.net.URI.create(target) : java.net.URI.create("dummyscheme://"
					 + target);
			}
			catch (System.ArgumentException)
			{
				throw new System.ArgumentException("Does not contain a valid host:port authority: "
					 + target + helpText);
			}
			string host = uri.getHost();
			int port = uri.getPort();
			if (port == -1)
			{
				port = defaultPort;
			}
			string path = uri.getPath();
			if ((host == null) || (port < 0) || (!hasScheme && path != null && !path.isEmpty(
				)))
			{
				throw new System.ArgumentException("Does not contain a valid host:port authority: "
					 + target + helpText);
			}
			return createSocketAddrForHost(host, port);
		}

		/// <summary>Create a socket address with the given host and port.</summary>
		/// <remarks>
		/// Create a socket address with the given host and port.  The hostname
		/// might be replaced with another host that was set via
		/// <see cref="addStaticResolution(string, string)"/>
		/// .  The value of
		/// hadoop.security.token.service.use_ip will determine whether the
		/// standard java host resolver is used, or if the fully qualified resolver
		/// is used.
		/// </remarks>
		/// <param name="host">the hostname or IP use to instantiate the object</param>
		/// <param name="port">the port number</param>
		/// <returns>InetSocketAddress</returns>
		public static java.net.InetSocketAddress createSocketAddrForHost(string host, int
			 port)
		{
			string staticHost = getStaticResolution(host);
			string resolveHost = (staticHost != null) ? staticHost : host;
			java.net.InetSocketAddress addr;
			try
			{
				java.net.InetAddress iaddr = org.apache.hadoop.security.SecurityUtil.getByName(resolveHost
					);
				// if there is a static entry for the host, make the returned
				// address look like the original given host
				if (staticHost != null)
				{
					iaddr = java.net.InetAddress.getByAddress(host, iaddr.getAddress());
				}
				addr = new java.net.InetSocketAddress(iaddr, port);
			}
			catch (java.net.UnknownHostException)
			{
				addr = java.net.InetSocketAddress.createUnresolved(host, port);
			}
			return addr;
		}

		/// <summary>Resolve the uri's hostname and add the default port if not in the uri</summary>
		/// <param name="uri">to resolve</param>
		/// <param name="defaultPort">if none is given</param>
		/// <returns>URI</returns>
		public static java.net.URI getCanonicalUri(java.net.URI uri, int defaultPort)
		{
			// skip if there is no authority, ie. "file" scheme or relative uri
			string host = uri.getHost();
			if (host == null)
			{
				return uri;
			}
			string fqHost = canonicalizeHost(host);
			int port = uri.getPort();
			// short out if already canonical with a port
			if (host.Equals(fqHost) && port != -1)
			{
				return uri;
			}
			// reconstruct the uri with the canonical host and port
			try
			{
				uri = new java.net.URI(uri.getScheme(), uri.getUserInfo(), fqHost, (port == -1) ? 
					defaultPort : port, uri.getPath(), uri.getQuery(), uri.getFragment());
			}
			catch (java.net.URISyntaxException e)
			{
				throw new System.ArgumentException(e);
			}
			return uri;
		}

		private static readonly java.util.concurrent.ConcurrentHashMap<string, string> canonicalizedHostCache
			 = new java.util.concurrent.ConcurrentHashMap<string, string>();

		// cache the canonicalized hostnames;  the cache currently isn't expired,
		// but the canonicals will only change if the host's resolver configuration
		// changes
		private static string canonicalizeHost(string host)
		{
			// check if the host has already been canonicalized
			string fqHost = canonicalizedHostCache[host];
			if (fqHost == null)
			{
				try
				{
					fqHost = org.apache.hadoop.security.SecurityUtil.getByName(host).getHostName();
					// slight race condition, but won't hurt
					canonicalizedHostCache.putIfAbsent(host, fqHost);
				}
				catch (java.net.UnknownHostException)
				{
					fqHost = host;
				}
			}
			return fqHost;
		}

		/// <summary>Adds a static resolution for host.</summary>
		/// <remarks>
		/// Adds a static resolution for host. This can be used for setting up
		/// hostnames with names that are fake to point to a well known host. For e.g.
		/// in some testcases we require to have daemons with different hostnames
		/// running on the same machine. In order to create connections to these
		/// daemons, one can set up mappings from those hostnames to "localhost".
		/// <see cref="getStaticResolution(string)"/>
		/// can be used to query for
		/// the actual hostname.
		/// </remarks>
		/// <param name="host"/>
		/// <param name="resolvedName"/>
		public static void addStaticResolution(string host, string resolvedName)
		{
			lock (hostToResolved)
			{
				hostToResolved[host] = resolvedName;
			}
		}

		/// <summary>Retrieves the resolved name for the passed host.</summary>
		/// <remarks>
		/// Retrieves the resolved name for the passed host. The resolved name must
		/// have been set earlier using
		/// <see cref="addStaticResolution(string, string)"/>
		/// </remarks>
		/// <param name="host"/>
		/// <returns>the resolution</returns>
		public static string getStaticResolution(string host)
		{
			lock (hostToResolved)
			{
				return hostToResolved[host];
			}
		}

		/// <summary>
		/// This is used to get all the resolutions that were added using
		/// <see cref="addStaticResolution(string, string)"/>
		/// . The return
		/// value is a List each element of which contains an array of String
		/// of the form String[0]=hostname, String[1]=resolved-hostname
		/// </summary>
		/// <returns>the list of resolutions</returns>
		public static System.Collections.Generic.IList<string[]> getAllStaticResolutions(
			)
		{
			lock (hostToResolved)
			{
				System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<string
					, string>> entries = hostToResolved;
				if (entries.Count == 0)
				{
					return null;
				}
				System.Collections.Generic.IList<string[]> l = new System.Collections.Generic.List
					<string[]>(entries.Count);
				foreach (System.Collections.Generic.KeyValuePair<string, string> e in entries)
				{
					l.add(new string[] { e.Key, e.Value });
				}
				return l;
			}
		}

		/// <summary>
		/// Returns InetSocketAddress that a client can use to
		/// connect to the server.
		/// </summary>
		/// <remarks>
		/// Returns InetSocketAddress that a client can use to
		/// connect to the server. Server.getListenerAddress() is not correct when
		/// the server binds to "0.0.0.0". This returns "hostname:port" of the server,
		/// or "127.0.0.1:port" when the getListenerAddress() returns "0.0.0.0:port".
		/// </remarks>
		/// <param name="server"/>
		/// <returns>socket address that a client can use to connect to the server.</returns>
		public static java.net.InetSocketAddress getConnectAddress(org.apache.hadoop.ipc.Server
			 server)
		{
			return getConnectAddress(server.getListenerAddress());
		}

		/// <summary>
		/// Returns an InetSocketAddress that a client can use to connect to the
		/// given listening address.
		/// </summary>
		/// <param name="addr">of a listener</param>
		/// <returns>socket address that a client can use to connect to the server.</returns>
		public static java.net.InetSocketAddress getConnectAddress(java.net.InetSocketAddress
			 addr)
		{
			if (!addr.isUnresolved() && addr.getAddress().isAnyLocalAddress())
			{
				try
				{
					addr = new java.net.InetSocketAddress(java.net.InetAddress.getLocalHost(), addr.getPort
						());
				}
				catch (java.net.UnknownHostException)
				{
					// shouldn't get here unless the host doesn't have a loopback iface
					addr = createSocketAddrForHost("127.0.0.1", addr.getPort());
				}
			}
			return addr;
		}

		/// <summary>
		/// Same as <code>getInputStream(socket, socket.getSoTimeout()).</code>
		/// <br /><br />
		/// </summary>
		/// <seealso cref="getInputStream(java.net.Socket, long)"/>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.net.SocketInputWrapper getInputStream(java.net.Socket
			 socket)
		{
			return getInputStream(socket, socket.getSoTimeout());
		}

		/// <summary>
		/// Return a
		/// <see cref="SocketInputWrapper"/>
		/// for the socket and set the given
		/// timeout. If the socket does not have an associated channel, then its socket
		/// timeout will be set to the specified value. Otherwise, a
		/// <see cref="SocketInputStream"/>
		/// will be created which reads with the configured
		/// timeout.
		/// Any socket created using socket factories returned by
		/// <see cref="NetUtils()"/>
		/// ,
		/// must use this interface instead of
		/// <see cref="java.net.Socket.getInputStream()"/>
		/// .
		/// In general, this should be called only once on each socket: see the note
		/// in
		/// <see cref="SocketInputWrapper.setTimeout(long)"/>
		/// for more information.
		/// </summary>
		/// <seealso cref="java.net.Socket.getChannel()"/>
		/// <param name="socket"/>
		/// <param name="timeout">
		/// timeout in milliseconds. zero for waiting as
		/// long as necessary.
		/// </param>
		/// <returns>SocketInputWrapper for reading from the socket.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.net.SocketInputWrapper getInputStream(java.net.Socket
			 socket, long timeout)
		{
			java.io.InputStream stm = (socket.getChannel() == null) ? socket.getInputStream()
				 : new org.apache.hadoop.net.SocketInputStream(socket);
			org.apache.hadoop.net.SocketInputWrapper w = new org.apache.hadoop.net.SocketInputWrapper
				(socket, stm);
			w.setTimeout(timeout);
			return w;
		}

		/// <summary>Same as getOutputStream(socket, 0).</summary>
		/// <remarks>
		/// Same as getOutputStream(socket, 0). Timeout of zero implies write will
		/// wait until data is available.<br /><br />
		/// From documentation for
		/// <see cref="getOutputStream(java.net.Socket, long)"/>
		/// : <br />
		/// Returns OutputStream for the socket. If the socket has an associated
		/// SocketChannel then it returns a
		/// <see cref="SocketOutputStream"/>
		/// with the given timeout. If the socket does not
		/// have a channel,
		/// <see cref="java.net.Socket.getOutputStream()"/>
		/// is returned. In the later
		/// case, the timeout argument is ignored and the write will wait until
		/// data is available.<br /><br />
		/// Any socket created using socket factories returned by
		/// <see cref="NetUtils"/>
		/// ,
		/// must use this interface instead of
		/// <see cref="java.net.Socket.getOutputStream()"/>
		/// .
		/// </remarks>
		/// <seealso cref="getOutputStream(java.net.Socket, long)"/>
		/// <param name="socket"/>
		/// <returns>OutputStream for writing to the socket.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static java.io.OutputStream getOutputStream(java.net.Socket socket)
		{
			return getOutputStream(socket, 0);
		}

		/// <summary>Returns OutputStream for the socket.</summary>
		/// <remarks>
		/// Returns OutputStream for the socket. If the socket has an associated
		/// SocketChannel then it returns a
		/// <see cref="SocketOutputStream"/>
		/// with the given timeout. If the socket does not
		/// have a channel,
		/// <see cref="java.net.Socket.getOutputStream()"/>
		/// is returned. In the later
		/// case, the timeout argument is ignored and the write will wait until
		/// data is available.<br /><br />
		/// Any socket created using socket factories returned by
		/// <see cref="NetUtils"/>
		/// ,
		/// must use this interface instead of
		/// <see cref="java.net.Socket.getOutputStream()"/>
		/// .
		/// </remarks>
		/// <seealso cref="java.net.Socket.getChannel()"/>
		/// <param name="socket"/>
		/// <param name="timeout">
		/// timeout in milliseconds. This may not always apply. zero
		/// for waiting as long as necessary.
		/// </param>
		/// <returns>OutputStream for writing to the socket.</returns>
		/// <exception cref="System.IO.IOException"></exception>
		public static java.io.OutputStream getOutputStream(java.net.Socket socket, long timeout
			)
		{
			return (socket.getChannel() == null) ? socket.getOutputStream() : new org.apache.hadoop.net.SocketOutputStream
				(socket, timeout);
		}

		/// <summary>
		/// This is a drop-in replacement for
		/// <see cref="java.net.Socket.connect(java.net.SocketAddress, int)"/>
		/// .
		/// In the case of normal sockets that don't have associated channels, this
		/// just invokes <code>socket.connect(endpoint, timeout)</code>. If
		/// <code>socket.getChannel()</code> returns a non-null channel,
		/// connect is implemented using Hadoop's selectors. This is done mainly
		/// to avoid Sun's connect implementation from creating thread-local
		/// selectors, since Hadoop does not have control on when these are closed
		/// and could end up taking all the available file descriptors.
		/// </summary>
		/// <seealso cref="java.net.Socket.connect(java.net.SocketAddress, int)"/>
		/// <param name="socket"/>
		/// <param name="address">the remote address</param>
		/// <param name="timeout">timeout in milliseconds</param>
		/// <exception cref="System.IO.IOException"/>
		public static void connect(java.net.Socket socket, java.net.SocketAddress address
			, int timeout)
		{
			connect(socket, address, null, timeout);
		}

		/// <summary>
		/// Like
		/// <see cref="connect(java.net.Socket, java.net.SocketAddress, int)"/>
		/// but
		/// also takes a local address and port to bind the socket to.
		/// </summary>
		/// <param name="socket"/>
		/// <param name="endpoint">the remote address</param>
		/// <param name="localAddr">the local address to bind the socket to</param>
		/// <param name="timeout">timeout in milliseconds</param>
		/// <exception cref="System.IO.IOException"/>
		public static void connect(java.net.Socket socket, java.net.SocketAddress endpoint
			, java.net.SocketAddress localAddr, int timeout)
		{
			if (socket == null || endpoint == null || timeout < 0)
			{
				throw new System.ArgumentException("Illegal argument for connect()");
			}
			java.nio.channels.SocketChannel ch = socket.getChannel();
			if (localAddr != null)
			{
				java.lang.Class localClass = Sharpen.Runtime.getClassForObject(localAddr);
				java.lang.Class remoteClass = Sharpen.Runtime.getClassForObject(endpoint);
				com.google.common.@base.Preconditions.checkArgument(localClass.Equals(remoteClass
					), "Local address %s must be of same family as remote address %s.", localAddr, endpoint
					);
				socket.bind(localAddr);
			}
			try
			{
				if (ch == null)
				{
					// let the default implementation handle it.
					socket.connect(endpoint, timeout);
				}
				else
				{
					org.apache.hadoop.net.SocketIOWithTimeout.connect(ch, endpoint, timeout);
				}
			}
			catch (java.net.SocketTimeoutException ste)
			{
				throw new org.apache.hadoop.net.ConnectTimeoutException(ste.Message);
			}
			// There is a very rare case allowed by the TCP specification, such that
			// if we are trying to connect to an endpoint on the local machine,
			// and we end up choosing an ephemeral port equal to the destination port,
			// we will actually end up getting connected to ourself (ie any data we
			// send just comes right back). This is only possible if the target
			// daemon is down, so we'll treat it like connection refused.
			if (socket.getLocalPort() == socket.getPort() && socket.getLocalAddress().Equals(
				socket.getInetAddress()))
			{
				LOG.info("Detected a loopback TCP socket, disconnecting it");
				socket.close();
				throw new java.net.ConnectException("Localhost targeted connection resulted in a loopback. "
					 + "No daemon is listening on the target port.");
			}
		}

		/// <summary>
		/// Given a string representation of a host, return its ip address
		/// in textual presentation.
		/// </summary>
		/// <param name="name">
		/// a string representation of a host:
		/// either a textual representation its IP address or its host name
		/// </param>
		/// <returns>its IP address in the string format</returns>
		public static string normalizeHostName(string name)
		{
			try
			{
				return java.net.InetAddress.getByName(name).getHostAddress();
			}
			catch (java.net.UnknownHostException)
			{
				return name;
			}
		}

		/// <summary>
		/// Given a collection of string representation of hosts, return a list of
		/// corresponding IP addresses in the textual representation.
		/// </summary>
		/// <param name="names">a collection of string representations of hosts</param>
		/// <returns>a list of corresponding IP addresses in the string format</returns>
		/// <seealso cref="normalizeHostName(string)"/>
		public static System.Collections.Generic.IList<string> normalizeHostNames(System.Collections.Generic.ICollection
			<string> names)
		{
			System.Collections.Generic.IList<string> hostNames = new System.Collections.Generic.List
				<string>(names.Count);
			foreach (string name in names)
			{
				hostNames.add(normalizeHostName(name));
			}
			return hostNames;
		}

		/// <summary>
		/// Performs a sanity check on the list of hostnames/IPs to verify they at least
		/// appear to be valid.
		/// </summary>
		/// <param name="names">- List of hostnames/IPs</param>
		/// <exception cref="java.net.UnknownHostException"/>
		public static void verifyHostnames(string[] names)
		{
			foreach (string name in names)
			{
				if (name == null)
				{
					throw new java.net.UnknownHostException("null hostname found");
				}
				// The first check supports URL formats (e.g. hdfs://, etc.). 
				// java.net.URI requires a schema, so we add a dummy one if it doesn't
				// have one already.
				java.net.URI uri = null;
				try
				{
					uri = new java.net.URI(name);
					if (uri.getHost() == null)
					{
						uri = new java.net.URI("http://" + name);
					}
				}
				catch (java.net.URISyntaxException)
				{
					uri = null;
				}
				if (uri == null || uri.getHost() == null)
				{
					throw new java.net.UnknownHostException(name + " is not a valid Inet address");
				}
			}
		}

		private static readonly java.util.regex.Pattern ipPortPattern = java.util.regex.Pattern
			.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(:\\d+)?");

		// Pattern for matching ip[:port]
		/// <summary>
		/// Attempt to obtain the host name of the given string which contains
		/// an IP address and an optional port.
		/// </summary>
		/// <param name="ipPort">string of form ip[:port]</param>
		/// <returns>Host name or null if the name can not be determined</returns>
		public static string getHostNameOfIP(string ipPort)
		{
			if (null == ipPort || !ipPortPattern.matcher(ipPort).matches())
			{
				return null;
			}
			try
			{
				int colonIdx = ipPort.IndexOf(':');
				string ip = (-1 == colonIdx) ? ipPort : Sharpen.Runtime.substring(ipPort, 0, ipPort
					.IndexOf(':'));
				return java.net.InetAddress.getByName(ip).getHostName();
			}
			catch (java.net.UnknownHostException)
			{
				return null;
			}
		}

		/// <summary>Return hostname without throwing exception.</summary>
		/// <returns>hostname</returns>
		public static string getHostname()
		{
			try
			{
				return string.Empty + java.net.InetAddress.getLocalHost();
			}
			catch (java.net.UnknownHostException uhe)
			{
				return string.Empty + uhe;
			}
		}

		/// <summary>Compose a "host:port" string from the address.</summary>
		public static string getHostPortString(java.net.InetSocketAddress addr)
		{
			return addr.getHostName() + ":" + addr.getPort();
		}

		/// <summary>
		/// Checks if
		/// <paramref name="host"/>
		/// is a local host name and return
		/// <see cref="java.net.InetAddress"/>
		/// corresponding to that address.
		/// </summary>
		/// <param name="host">the specified host</param>
		/// <returns>
		/// a valid local
		/// <see cref="java.net.InetAddress"/>
		/// or null
		/// </returns>
		/// <exception cref="System.Net.Sockets.SocketException">if an I/O error occurs</exception>
		public static java.net.InetAddress getLocalInetAddress(string host)
		{
			if (host == null)
			{
				return null;
			}
			java.net.InetAddress addr = null;
			try
			{
				addr = org.apache.hadoop.security.SecurityUtil.getByName(host);
				if (java.net.NetworkInterface.getByInetAddress(addr) == null)
				{
					addr = null;
				}
			}
			catch (java.net.UnknownHostException)
			{
			}
			// Not a local address
			return addr;
		}

		/// <summary>
		/// Given an InetAddress, checks to see if the address is a local address, by
		/// comparing the address with all the interfaces on the node.
		/// </summary>
		/// <param name="addr">address to check if it is local node's address</param>
		/// <returns>true if the address corresponds to the local node</returns>
		public static bool isLocalAddress(java.net.InetAddress addr)
		{
			// Check if the address is any local or loop back
			bool local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();
			// Check if the address is defined on any interface
			if (!local)
			{
				try
				{
					local = java.net.NetworkInterface.getByInetAddress(addr) != null;
				}
				catch (System.Net.Sockets.SocketException)
				{
					local = false;
				}
			}
			return local;
		}

		/// <summary>
		/// Take an IOException , the local host port and remote host port details and
		/// return an IOException with the input exception as the cause and also
		/// include the host details.
		/// </summary>
		/// <remarks>
		/// Take an IOException , the local host port and remote host port details and
		/// return an IOException with the input exception as the cause and also
		/// include the host details. The new exception provides the stack trace of the
		/// place where the exception is thrown and some extra diagnostics information.
		/// If the exception is BindException or ConnectException or
		/// UnknownHostException or SocketTimeoutException, return a new one of the
		/// same type; Otherwise return an IOException.
		/// </remarks>
		/// <param name="destHost">target host (nullable)</param>
		/// <param name="destPort">target port</param>
		/// <param name="localHost">local host (nullable)</param>
		/// <param name="localPort">local port</param>
		/// <param name="exception">the caught exception.</param>
		/// <returns>an exception to throw</returns>
		public static System.IO.IOException wrapException(string destHost, int destPort, 
			string localHost, int localPort, System.IO.IOException exception)
		{
			if (exception is java.net.BindException)
			{
				return wrapWithMessage(exception, "Problem binding to [" + localHost + ":" + localPort
					 + "] " + exception + ";" + see("BindException"));
			}
			else
			{
				if (exception is java.net.ConnectException)
				{
					// connection refused; include the host:port in the error
					return wrapWithMessage(exception, "Call From " + localHost + " to " + destHost + 
						":" + destPort + " failed on connection exception: " + exception + ";" + see("ConnectionRefused"
						));
				}
				else
				{
					if (exception is java.net.UnknownHostException)
					{
						return wrapWithMessage(exception, "Invalid host name: " + getHostDetailsAsString(
							destHost, destPort, localHost) + exception + ";" + see("UnknownHost"));
					}
					else
					{
						if (exception is java.net.SocketTimeoutException)
						{
							return wrapWithMessage(exception, "Call From " + localHost + " to " + destHost + 
								":" + destPort + " failed on socket timeout exception: " + exception + ";" + see
								("SocketTimeout"));
						}
						else
						{
							if (exception is java.net.NoRouteToHostException)
							{
								return wrapWithMessage(exception, "No Route to Host from  " + localHost + " to " 
									+ destHost + ":" + destPort + " failed on socket timeout exception: " + exception
									 + ";" + see("NoRouteToHost"));
							}
							else
							{
								if (exception is java.io.EOFException)
								{
									return wrapWithMessage(exception, "End of File Exception between " + getHostDetailsAsString
										(destHost, destPort, localHost) + ": " + exception + ";" + see("EOFException"));
								}
								else
								{
									return (System.IO.IOException)new System.IO.IOException("Failed on local exception: "
										 + exception + "; Host Details : " + getHostDetailsAsString(destHost, destPort, 
										localHost)).initCause(exception);
								}
							}
						}
					}
				}
			}
		}

		private static string see(string entry)
		{
			return FOR_MORE_DETAILS_SEE + HADOOP_WIKI + entry;
		}

		private static T wrapWithMessage<T>(T exception, string msg)
			where T : System.IO.IOException
		{
			java.lang.Class clazz = Sharpen.Runtime.getClassForObject(exception);
			try
			{
				java.lang.reflect.Constructor<System.Exception> ctor = clazz.getConstructor(Sharpen.Runtime.getClassForType
					(typeof(string)));
				System.Exception t = ctor.newInstance(msg);
				return (T)(t.initCause(exception));
			}
			catch (System.Exception e)
			{
				LOG.warn("Unable to wrap exception of type " + clazz + ": it has no (String) constructor"
					, e);
				return exception;
			}
		}

		/// <summary>Get the host details as a string</summary>
		/// <param name="destHost">destinatioon host (nullable)</param>
		/// <param name="destPort">destination port</param>
		/// <param name="localHost">local host (nullable)</param>
		/// <returns>a string describing the destination host:port and the local host</returns>
		private static string getHostDetailsAsString(string destHost, int destPort, string
			 localHost)
		{
			java.lang.StringBuilder hostDetails = new java.lang.StringBuilder(27);
			hostDetails.Append("local host is: ").Append(quoteHost(localHost)).Append("; ");
			hostDetails.Append("destination host is: ").Append(quoteHost(destHost)).Append(":"
				).Append(destPort).Append("; ");
			return hostDetails.ToString();
		}

		/// <summary>Quote a hostname if it is not null</summary>
		/// <param name="hostname">the hostname; nullable</param>
		/// <returns>
		/// a quoted hostname or
		/// <see cref="UNKNOWN_HOST"/>
		/// if the hostname is null
		/// </returns>
		private static string quoteHost(string hostname)
		{
			return (hostname != null) ? ("\"" + hostname + "\"") : UNKNOWN_HOST;
		}

		/// <returns>
		/// true if the given string is a subnet specified
		/// using CIDR notation, false otherwise
		/// </returns>
		public static bool isValidSubnet(string subnet)
		{
			try
			{
				new org.apache.commons.net.util.SubnetUtils(subnet);
				return true;
			}
			catch (System.ArgumentException)
			{
				return false;
			}
		}

		/// <summary>
		/// Add all addresses associated with the given nif in the
		/// given subnet to the given list.
		/// </summary>
		private static void addMatchingAddrs(java.net.NetworkInterface nif, org.apache.commons.net.util.SubnetUtils.SubnetInfo
			 subnetInfo, System.Collections.Generic.IList<java.net.InetAddress> addrs)
		{
			java.util.Enumeration<java.net.InetAddress> ifAddrs = nif.getInetAddresses();
			while (ifAddrs.MoveNext())
			{
				java.net.InetAddress ifAddr = ifAddrs.Current;
				if (subnetInfo.isInRange(ifAddr.getHostAddress()))
				{
					addrs.add(ifAddr);
				}
			}
		}

		/// <summary>
		/// Return an InetAddress for each interface that matches the
		/// given subnet specified using CIDR notation.
		/// </summary>
		/// <param name="subnet">subnet specified using CIDR notation</param>
		/// <param name="returnSubinterfaces">whether to return IPs associated with subinterfaces
		/// 	</param>
		/// <exception cref="System.ArgumentException">if subnet is invalid</exception>
		public static System.Collections.Generic.IList<java.net.InetAddress> getIPs(string
			 subnet, bool returnSubinterfaces)
		{
			System.Collections.Generic.IList<java.net.InetAddress> addrs = new System.Collections.Generic.List
				<java.net.InetAddress>();
			org.apache.commons.net.util.SubnetUtils.SubnetInfo subnetInfo = new org.apache.commons.net.util.SubnetUtils
				(subnet).getInfo();
			java.util.Enumeration<java.net.NetworkInterface> nifs;
			try
			{
				nifs = java.net.NetworkInterface.getNetworkInterfaces();
			}
			catch (System.Net.Sockets.SocketException e)
			{
				LOG.error("Unable to get host interfaces", e);
				return addrs;
			}
			while (nifs.MoveNext())
			{
				java.net.NetworkInterface nif = nifs.Current;
				// NB: adding addresses even if the nif is not up
				addMatchingAddrs(nif, subnetInfo, addrs);
				if (!returnSubinterfaces)
				{
					continue;
				}
				java.util.Enumeration<java.net.NetworkInterface> subNifs = nif.getSubInterfaces();
				while (subNifs.MoveNext())
				{
					addMatchingAddrs(subNifs.Current, subnetInfo, addrs);
				}
			}
			return addrs;
		}

		/// <summary>Return a free port number.</summary>
		/// <remarks>
		/// Return a free port number. There is no guarantee it will remain free, so
		/// it should be used immediately.
		/// </remarks>
		/// <returns>A free port for binding a local socket</returns>
		public static int getFreeSocketPort()
		{
			int port = 0;
			try
			{
				java.net.ServerSocket s = new java.net.ServerSocket(0);
				port = s.getLocalPort();
				s.close();
				return port;
			}
			catch (System.IO.IOException)
			{
			}
			// Could not get a free port. Return default port 0.
			return port;
		}
	}
}
