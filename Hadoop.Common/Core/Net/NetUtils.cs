using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Com.Google.Common.Base;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Util;
using Javax.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Net.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;

using Reflect;

namespace Org.Apache.Hadoop.Net
{
	public class NetUtils
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(NetUtils));

		private static IDictionary<string, string> hostToResolved = new Dictionary<string
			, string>();

		/// <summary>
		/// text to point users elsewhere:
		/// <value/>
		/// 
		/// </summary>
		private const string ForMoreDetailsSee = " For more details see:  ";

		/// <summary>
		/// text included in wrapped exceptions if the host is null:
		/// <value/>
		/// 
		/// </summary>
		public const string UnknownHost = "(unknown)";

		/// <summary>
		/// Base URL of the Hadoop Wiki:
		/// <value/>
		/// 
		/// </summary>
		public const string HadoopWiki = "http://wiki.apache.org/hadoop/";

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
		/// <see cref="Org.Apache.Hadoop.Ipc.VersionedProtocol"/>
		/// )
		/// </param>
		/// <returns>a socket factory</returns>
		public static SocketFactory GetSocketFactory(Configuration conf, Type clazz)
		{
			SocketFactory factory = null;
			string propValue = conf.Get("hadoop.rpc.socket.factory.class." + clazz.Name);
			if ((propValue != null) && (propValue.Length > 0))
			{
				factory = GetSocketFactoryFromProperty(conf, propValue);
			}
			if (factory == null)
			{
				factory = GetDefaultSocketFactory(conf);
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
		public static SocketFactory GetDefaultSocketFactory(Configuration conf)
		{
			string propValue = conf.Get(CommonConfigurationKeysPublic.HadoopRpcSocketFactoryClassDefaultKey
				, CommonConfigurationKeysPublic.HadoopRpcSocketFactoryClassDefaultDefault);
			if ((propValue == null) || (propValue.Length == 0))
			{
				return SocketFactory.GetDefault();
			}
			return GetSocketFactoryFromProperty(conf, propValue);
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
		public static SocketFactory GetSocketFactoryFromProperty(Configuration conf, string
			 propValue)
		{
			try
			{
				Type theClass = conf.GetClassByName(propValue);
				return (SocketFactory)ReflectionUtils.NewInstance(theClass, conf);
			}
			catch (TypeLoadException cnfe)
			{
				throw new RuntimeException("Socket Factory class not found: " + cnfe);
			}
		}

		/// <summary>
		/// Util method to build socket addr from either:
		/// <host>:<port>
		/// <fs>://<host>:<port>/<path>
		/// </summary>
		public static IPEndPoint CreateSocketAddr(string target)
		{
			return CreateSocketAddr(target, -1);
		}

		/// <summary>
		/// Util method to build socket addr from either:
		/// <host>
		/// <host>:<port>
		/// <fs>://<host>:<port>/<path>
		/// </summary>
		public static IPEndPoint CreateSocketAddr(string target, int defaultPort)
		{
			return CreateSocketAddr(target, defaultPort, null);
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
		public static IPEndPoint CreateSocketAddr(string target, int defaultPort, string 
			configName)
		{
			string helpText = string.Empty;
			if (configName != null)
			{
				helpText = " (configuration property '" + configName + "')";
			}
			if (target == null)
			{
				throw new ArgumentException("Target address cannot be null." + helpText);
			}
			target = target.Trim();
			bool hasScheme = target.Contains("://");
			URI uri = null;
			try
			{
				uri = hasScheme ? URI.Create(target) : URI.Create("dummyscheme://" + target);
			}
			catch (ArgumentException)
			{
				throw new ArgumentException("Does not contain a valid host:port authority: " + target
					 + helpText);
			}
			string host = uri.GetHost();
			int port = uri.GetPort();
			if (port == -1)
			{
				port = defaultPort;
			}
			string path = uri.GetPath();
			if ((host == null) || (port < 0) || (!hasScheme && path != null && !path.IsEmpty(
				)))
			{
				throw new ArgumentException("Does not contain a valid host:port authority: " + target
					 + helpText);
			}
			return CreateSocketAddrForHost(host, port);
		}

		/// <summary>Create a socket address with the given host and port.</summary>
		/// <remarks>
		/// Create a socket address with the given host and port.  The hostname
		/// might be replaced with another host that was set via
		/// <see cref="AddStaticResolution(string, string)"/>
		/// .  The value of
		/// hadoop.security.token.service.use_ip will determine whether the
		/// standard java host resolver is used, or if the fully qualified resolver
		/// is used.
		/// </remarks>
		/// <param name="host">the hostname or IP use to instantiate the object</param>
		/// <param name="port">the port number</param>
		/// <returns>InetSocketAddress</returns>
		public static IPEndPoint CreateSocketAddrForHost(string host, int port)
		{
			string staticHost = GetStaticResolution(host);
			string resolveHost = (staticHost != null) ? staticHost : host;
			IPEndPoint addr;
			try
			{
				IPAddress iaddr = SecurityUtil.GetByName(resolveHost);
				// if there is a static entry for the host, make the returned
				// address look like the original given host
				if (staticHost != null)
				{
					iaddr = IPAddress.GetByAddress(host, iaddr.GetAddressBytes());
				}
				addr = new IPEndPoint(iaddr, port);
			}
			catch (UnknownHostException)
			{
				addr = IPEndPoint.CreateUnresolved(host, port);
			}
			return addr;
		}

		/// <summary>Resolve the uri's hostname and add the default port if not in the uri</summary>
		/// <param name="uri">to resolve</param>
		/// <param name="defaultPort">if none is given</param>
		/// <returns>URI</returns>
		public static URI GetCanonicalUri(URI uri, int defaultPort)
		{
			// skip if there is no authority, ie. "file" scheme or relative uri
			string host = uri.GetHost();
			if (host == null)
			{
				return uri;
			}
			string fqHost = CanonicalizeHost(host);
			int port = uri.GetPort();
			// short out if already canonical with a port
			if (host.Equals(fqHost) && port != -1)
			{
				return uri;
			}
			// reconstruct the uri with the canonical host and port
			try
			{
				uri = new URI(uri.GetScheme(), uri.GetUserInfo(), fqHost, (port == -1) ? defaultPort
					 : port, uri.GetPath(), uri.GetQuery(), uri.GetFragment());
			}
			catch (URISyntaxException e)
			{
				throw new ArgumentException(e);
			}
			return uri;
		}

		private static readonly ConcurrentHashMap<string, string> canonicalizedHostCache = 
			new ConcurrentHashMap<string, string>();

		// cache the canonicalized hostnames;  the cache currently isn't expired,
		// but the canonicals will only change if the host's resolver configuration
		// changes
		private static string CanonicalizeHost(string host)
		{
			// check if the host has already been canonicalized
			string fqHost = canonicalizedHostCache[host];
			if (fqHost == null)
			{
				try
				{
					fqHost = SecurityUtil.GetByName(host).GetHostName();
					// slight race condition, but won't hurt
					canonicalizedHostCache.PutIfAbsent(host, fqHost);
				}
				catch (UnknownHostException)
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
		/// <see cref="GetStaticResolution(string)"/>
		/// can be used to query for
		/// the actual hostname.
		/// </remarks>
		/// <param name="host"/>
		/// <param name="resolvedName"/>
		public static void AddStaticResolution(string host, string resolvedName)
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
		/// <see cref="AddStaticResolution(string, string)"/>
		/// </remarks>
		/// <param name="host"/>
		/// <returns>the resolution</returns>
		public static string GetStaticResolution(string host)
		{
			lock (hostToResolved)
			{
				return hostToResolved[host];
			}
		}

		/// <summary>
		/// This is used to get all the resolutions that were added using
		/// <see cref="AddStaticResolution(string, string)"/>
		/// . The return
		/// value is a List each element of which contains an array of String
		/// of the form String[0]=hostname, String[1]=resolved-hostname
		/// </summary>
		/// <returns>the list of resolutions</returns>
		public static IList<string[]> GetAllStaticResolutions()
		{
			lock (hostToResolved)
			{
				ICollection<KeyValuePair<string, string>> entries = hostToResolved;
				if (entries.Count == 0)
				{
					return null;
				}
				IList<string[]> l = new AList<string[]>(entries.Count);
				foreach (KeyValuePair<string, string> e in entries)
				{
					l.AddItem(new string[] { e.Key, e.Value });
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
		public static IPEndPoint GetConnectAddress(Server server)
		{
			return GetConnectAddress(server.GetListenerAddress());
		}

		/// <summary>
		/// Returns an InetSocketAddress that a client can use to connect to the
		/// given listening address.
		/// </summary>
		/// <param name="addr">of a listener</param>
		/// <returns>socket address that a client can use to connect to the server.</returns>
		public static IPEndPoint GetConnectAddress(IPEndPoint addr)
		{
			if (!addr.IsUnresolved() && addr.Address.IsAnyLocalAddress())
			{
				try
				{
					addr = new IPEndPoint(Runtime.GetLocalHost(), addr.Port);
				}
				catch (UnknownHostException)
				{
					// shouldn't get here unless the host doesn't have a loopback iface
					addr = CreateSocketAddrForHost("127.0.0.1", addr.Port);
				}
			}
			return addr;
		}

		/// <summary>
		/// Same as <code>getInputStream(socket, socket.getSoTimeout()).</code>
		/// <br /><br />
		/// </summary>
		/// <seealso cref="GetInputStream(System.Net.Sockets.Socket, long)"/>
		/// <exception cref="System.IO.IOException"/>
		public static SocketInputWrapper GetInputStream(Socket socket)
		{
			return GetInputStream(socket, socket.ReceiveTimeout);
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
		/// <see cref="System.Net.Sockets.Socket.GetInputStream()"/>
		/// .
		/// In general, this should be called only once on each socket: see the note
		/// in
		/// <see cref="SocketInputWrapper.SetTimeout(long)"/>
		/// for more information.
		/// </summary>
		/// <seealso cref="System.Net.Sockets.Socket.GetChannel()"/>
		/// <param name="socket"/>
		/// <param name="timeout">
		/// timeout in milliseconds. zero for waiting as
		/// long as necessary.
		/// </param>
		/// <returns>SocketInputWrapper for reading from the socket.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static SocketInputWrapper GetInputStream(Socket socket, long timeout)
		{
			InputStream stm = (socket.GetChannel() == null) ? socket.GetInputStream() : new SocketInputStream
				(socket);
			SocketInputWrapper w = new SocketInputWrapper(socket, stm);
			w.SetTimeout(timeout);
			return w;
		}

		/// <summary>Same as getOutputStream(socket, 0).</summary>
		/// <remarks>
		/// Same as getOutputStream(socket, 0). Timeout of zero implies write will
		/// wait until data is available.<br /><br />
		/// From documentation for
		/// <see cref="GetOutputStream(System.Net.Sockets.Socket, long)"/>
		/// : <br />
		/// Returns OutputStream for the socket. If the socket has an associated
		/// SocketChannel then it returns a
		/// <see cref="SocketOutputStream"/>
		/// with the given timeout. If the socket does not
		/// have a channel,
		/// <see cref="System.Net.Sockets.Socket.GetOutputStream()"/>
		/// is returned. In the later
		/// case, the timeout argument is ignored and the write will wait until
		/// data is available.<br /><br />
		/// Any socket created using socket factories returned by
		/// <see cref="NetUtils"/>
		/// ,
		/// must use this interface instead of
		/// <see cref="System.Net.Sockets.Socket.GetOutputStream()"/>
		/// .
		/// </remarks>
		/// <seealso cref="GetOutputStream(System.Net.Sockets.Socket, long)"/>
		/// <param name="socket"/>
		/// <returns>OutputStream for writing to the socket.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static OutputStream GetOutputStream(Socket socket)
		{
			return GetOutputStream(socket, 0);
		}

		/// <summary>Returns OutputStream for the socket.</summary>
		/// <remarks>
		/// Returns OutputStream for the socket. If the socket has an associated
		/// SocketChannel then it returns a
		/// <see cref="SocketOutputStream"/>
		/// with the given timeout. If the socket does not
		/// have a channel,
		/// <see cref="System.Net.Sockets.Socket.GetOutputStream()"/>
		/// is returned. In the later
		/// case, the timeout argument is ignored and the write will wait until
		/// data is available.<br /><br />
		/// Any socket created using socket factories returned by
		/// <see cref="NetUtils"/>
		/// ,
		/// must use this interface instead of
		/// <see cref="System.Net.Sockets.Socket.GetOutputStream()"/>
		/// .
		/// </remarks>
		/// <seealso cref="System.Net.Sockets.Socket.GetChannel()"/>
		/// <param name="socket"/>
		/// <param name="timeout">
		/// timeout in milliseconds. This may not always apply. zero
		/// for waiting as long as necessary.
		/// </param>
		/// <returns>OutputStream for writing to the socket.</returns>
		/// <exception cref="System.IO.IOException"></exception>
		public static OutputStream GetOutputStream(Socket socket, long timeout)
		{
			return (socket.GetChannel() == null) ? socket.GetOutputStream() : new SocketOutputStream
				(socket, timeout);
		}

		/// <summary>
		/// This is a drop-in replacement for
		/// <see cref="System.Net.Sockets.Socket.Connect(System.Net.EndPoint, int)"/>
		/// .
		/// In the case of normal sockets that don't have associated channels, this
		/// just invokes <code>socket.connect(endpoint, timeout)</code>. If
		/// <code>socket.getChannel()</code> returns a non-null channel,
		/// connect is implemented using Hadoop's selectors. This is done mainly
		/// to avoid Sun's connect implementation from creating thread-local
		/// selectors, since Hadoop does not have control on when these are closed
		/// and could end up taking all the available file descriptors.
		/// </summary>
		/// <seealso cref="System.Net.Sockets.Socket.Connect(System.Net.EndPoint, int)"/>
		/// <param name="socket"/>
		/// <param name="address">the remote address</param>
		/// <param name="timeout">timeout in milliseconds</param>
		/// <exception cref="System.IO.IOException"/>
		public static void Connect(Socket socket, EndPoint address, int timeout)
		{
			Connect(socket, address, null, timeout);
		}

		/// <summary>
		/// Like
		/// <see cref="Connect(System.Net.Sockets.Socket, System.Net.EndPoint, int)"/>
		/// but
		/// also takes a local address and port to bind the socket to.
		/// </summary>
		/// <param name="socket"/>
		/// <param name="endpoint">the remote address</param>
		/// <param name="localAddr">the local address to bind the socket to</param>
		/// <param name="timeout">timeout in milliseconds</param>
		/// <exception cref="System.IO.IOException"/>
		public static void Connect(Socket socket, EndPoint endpoint, EndPoint localAddr, 
			int timeout)
		{
			if (socket == null || endpoint == null || timeout < 0)
			{
				throw new ArgumentException("Illegal argument for connect()");
			}
			SocketChannel ch = socket.GetChannel();
			if (localAddr != null)
			{
				Type localClass = localAddr.GetType();
				Type remoteClass = endpoint.GetType();
				Preconditions.CheckArgument(localClass.Equals(remoteClass), "Local address %s must be of same family as remote address %s."
					, localAddr, endpoint);
				socket.Bind2(localAddr);
			}
			try
			{
				if (ch == null)
				{
					// let the default implementation handle it.
					socket.Connect(endpoint, timeout);
				}
				else
				{
					SocketIOWithTimeout.Connect(ch, endpoint, timeout);
				}
			}
			catch (SocketTimeoutException ste)
			{
				throw new ConnectTimeoutException(ste.Message);
			}
			// There is a very rare case allowed by the TCP specification, such that
			// if we are trying to connect to an endpoint on the local machine,
			// and we end up choosing an ephemeral port equal to the destination port,
			// we will actually end up getting connected to ourself (ie any data we
			// send just comes right back). This is only possible if the target
			// daemon is down, so we'll treat it like connection refused.
			if (socket.GetLocalPort() == socket.GetPort() && socket.GetLocalAddress().Equals(
				socket.GetInetAddress()))
			{
				Log.Info("Detected a loopback TCP socket, disconnecting it");
				socket.Close();
				throw new ConnectException("Localhost targeted connection resulted in a loopback. "
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
		public static string NormalizeHostName(string name)
		{
			try
			{
				return Extensions.GetAddressByName(name).GetHostAddress();
			}
			catch (UnknownHostException)
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
		/// <seealso cref="NormalizeHostName(string)"/>
		public static IList<string> NormalizeHostNames(ICollection<string> names)
		{
			IList<string> hostNames = new AList<string>(names.Count);
			foreach (string name in names)
			{
				hostNames.AddItem(NormalizeHostName(name));
			}
			return hostNames;
		}

		/// <summary>
		/// Performs a sanity check on the list of hostnames/IPs to verify they at least
		/// appear to be valid.
		/// </summary>
		/// <param name="names">- List of hostnames/IPs</param>
		/// <exception cref="UnknownHostException"/>
		public static void VerifyHostnames(string[] names)
		{
			foreach (string name in names)
			{
				if (name == null)
				{
					throw new UnknownHostException("null hostname found");
				}
				// The first check supports URL formats (e.g. hdfs://, etc.). 
				// java.net.URI requires a schema, so we add a dummy one if it doesn't
				// have one already.
				URI uri = null;
				try
				{
					uri = new URI(name);
					if (uri.GetHost() == null)
					{
						uri = new URI("http://" + name);
					}
				}
				catch (URISyntaxException)
				{
					uri = null;
				}
				if (uri == null || uri.GetHost() == null)
				{
					throw new UnknownHostException(name + " is not a valid Inet address");
				}
			}
		}

		private static readonly Pattern ipPortPattern = Pattern.Compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(:\\d+)?"
			);

		// Pattern for matching ip[:port]
		/// <summary>
		/// Attempt to obtain the host name of the given string which contains
		/// an IP address and an optional port.
		/// </summary>
		/// <param name="ipPort">string of form ip[:port]</param>
		/// <returns>Host name or null if the name can not be determined</returns>
		public static string GetHostNameOfIP(string ipPort)
		{
			if (null == ipPort || !ipPortPattern.Matcher(ipPort).Matches())
			{
				return null;
			}
			try
			{
				int colonIdx = ipPort.IndexOf(':');
				string ip = (-1 == colonIdx) ? ipPort : Runtime.Substring(ipPort, 0, ipPort
					.IndexOf(':'));
				return Extensions.GetAddressByName(ip).GetHostName();
			}
			catch (UnknownHostException)
			{
				return null;
			}
		}

		/// <summary>Return hostname without throwing exception.</summary>
		/// <returns>hostname</returns>
		public static string GetHostname()
		{
			try
			{
				return string.Empty + Runtime.GetLocalHost();
			}
			catch (UnknownHostException uhe)
			{
				return string.Empty + uhe;
			}
		}

		/// <summary>Compose a "host:port" string from the address.</summary>
		public static string GetHostPortString(IPEndPoint addr)
		{
			return addr.GetHostName() + ":" + addr.Port;
		}

		/// <summary>
		/// Checks if
		/// <paramref name="host"/>
		/// is a local host name and return
		/// <see cref="System.Net.IPAddress"/>
		/// corresponding to that address.
		/// </summary>
		/// <param name="host">the specified host</param>
		/// <returns>
		/// a valid local
		/// <see cref="System.Net.IPAddress"/>
		/// or null
		/// </returns>
		/// <exception cref="System.Net.Sockets.SocketException">if an I/O error occurs</exception>
		public static IPAddress GetLocalInetAddress(string host)
		{
			if (host == null)
			{
				return null;
			}
			IPAddress addr = null;
			try
			{
				addr = SecurityUtil.GetByName(host);
				if (NetworkInterface.GetByInetAddress(addr) == null)
				{
					addr = null;
				}
			}
			catch (UnknownHostException)
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
		public static bool IsLocalAddress(IPAddress addr)
		{
			// Check if the address is any local or loop back
			bool local = addr.IsAnyLocalAddress() || addr.IsLoopbackAddress();
			// Check if the address is defined on any interface
			if (!local)
			{
				try
				{
					local = NetworkInterface.GetByInetAddress(addr) != null;
				}
				catch (SocketException)
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
		public static IOException WrapException(string destHost, int destPort, string localHost
			, int localPort, IOException exception)
		{
			if (exception is BindException)
			{
				return WrapWithMessage(exception, "Problem binding to [" + localHost + ":" + localPort
					 + "] " + exception + ";" + See("BindException"));
			}
			else
			{
				if (exception is ConnectException)
				{
					// connection refused; include the host:port in the error
					return WrapWithMessage(exception, "Call From " + localHost + " to " + destHost + 
						":" + destPort + " failed on connection exception: " + exception + ";" + See("ConnectionRefused"
						));
				}
				else
				{
					if (exception is UnknownHostException)
					{
						return WrapWithMessage(exception, "Invalid host name: " + GetHostDetailsAsString(
							destHost, destPort, localHost) + exception + ";" + See("UnknownHost"));
					}
					else
					{
						if (exception is SocketTimeoutException)
						{
							return WrapWithMessage(exception, "Call From " + localHost + " to " + destHost + 
								":" + destPort + " failed on socket timeout exception: " + exception + ";" + See
								("SocketTimeout"));
						}
						else
						{
							if (exception is NoRouteToHostException)
							{
								return WrapWithMessage(exception, "No Route to Host from  " + localHost + " to " 
									+ destHost + ":" + destPort + " failed on socket timeout exception: " + exception
									 + ";" + See("NoRouteToHost"));
							}
							else
							{
								if (exception is EOFException)
								{
									return WrapWithMessage(exception, "End of File Exception between " + GetHostDetailsAsString
										(destHost, destPort, localHost) + ": " + exception + ";" + See("EOFException"));
								}
								else
								{
									return (IOException)Extensions.InitCause(new IOException("Failed on local exception: "
										 + exception + "; Host Details : " + GetHostDetailsAsString(destHost, destPort, 
										localHost)), exception);
								}
							}
						}
					}
				}
			}
		}

		private static string See(string entry)
		{
			return ForMoreDetailsSee + HadoopWiki + entry;
		}

		private static T WrapWithMessage<T>(T exception, string msg)
			where T : IOException
		{
			Type clazz = exception.GetType();
			try
			{
				Constructor<Exception> ctor = clazz.GetConstructor(typeof(string));
				Exception t = ctor.NewInstance(msg);
				return (T)(Extensions.InitCause(t, exception));
			}
			catch (Exception e)
			{
				Log.Warn("Unable to wrap exception of type " + clazz + ": it has no (String) constructor"
					, e);
				return exception;
			}
		}

		/// <summary>Get the host details as a string</summary>
		/// <param name="destHost">destinatioon host (nullable)</param>
		/// <param name="destPort">destination port</param>
		/// <param name="localHost">local host (nullable)</param>
		/// <returns>a string describing the destination host:port and the local host</returns>
		private static string GetHostDetailsAsString(string destHost, int destPort, string
			 localHost)
		{
			StringBuilder hostDetails = new StringBuilder(27);
			hostDetails.Append("local host is: ").Append(QuoteHost(localHost)).Append("; ");
			hostDetails.Append("destination host is: ").Append(QuoteHost(destHost)).Append(":"
				).Append(destPort).Append("; ");
			return hostDetails.ToString();
		}

		/// <summary>Quote a hostname if it is not null</summary>
		/// <param name="hostname">the hostname; nullable</param>
		/// <returns>
		/// a quoted hostname or
		/// <see cref="UnknownHost"/>
		/// if the hostname is null
		/// </returns>
		private static string QuoteHost(string hostname)
		{
			return (hostname != null) ? ("\"" + hostname + "\"") : UnknownHost;
		}

		/// <returns>
		/// true if the given string is a subnet specified
		/// using CIDR notation, false otherwise
		/// </returns>
		public static bool IsValidSubnet(string subnet)
		{
			try
			{
				new SubnetUtils(subnet);
				return true;
			}
			catch (ArgumentException)
			{
				return false;
			}
		}

		/// <summary>
		/// Add all addresses associated with the given nif in the
		/// given subnet to the given list.
		/// </summary>
		private static void AddMatchingAddrs(NetworkInterface nif, SubnetUtils.SubnetInfo
			 subnetInfo, IList<IPAddress> addrs)
		{
			Enumeration<IPAddress> ifAddrs = nif.GetInetAddresses();
			while (ifAddrs.MoveNext())
			{
				IPAddress ifAddr = ifAddrs.Current;
				if (subnetInfo.IsInRange(ifAddr.GetHostAddress()))
				{
					addrs.AddItem(ifAddr);
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
		public static IList<IPAddress> GetIPs(string subnet, bool returnSubinterfaces)
		{
			IList<IPAddress> addrs = new AList<IPAddress>();
			SubnetUtils.SubnetInfo subnetInfo = new SubnetUtils(subnet).GetInfo();
			Enumeration<NetworkInterface> nifs;
			try
			{
				nifs = NetworkInterface.GetNetworkInterfaces();
			}
			catch (SocketException e)
			{
				Log.Error("Unable to get host interfaces", e);
				return addrs;
			}
			while (nifs.MoveNext())
			{
				NetworkInterface nif = nifs.Current;
				// NB: adding addresses even if the nif is not up
				AddMatchingAddrs(nif, subnetInfo, addrs);
				if (!returnSubinterfaces)
				{
					continue;
				}
				Enumeration<NetworkInterface> subNifs = nif.GetSubInterfaces();
				while (subNifs.MoveNext())
				{
					AddMatchingAddrs(subNifs.Current, subnetInfo, addrs);
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
		public static int GetFreeSocketPort()
		{
			int port = 0;
			try
			{
				Socket s = Extensions.CreateServerSocket(0);
				port = s.GetLocalPort();
				s.Close();
				return port;
			}
			catch (IOException)
			{
			}
			// Could not get a free port. Return default port 0.
			return port;
		}
	}
}
