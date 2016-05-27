using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	public class TestNetUtils
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestNetUtils));

		private const int DestPort = 4040;

		private static readonly string DestPortName = Sharpen.Extensions.ToString(DestPort
			);

		private const int LocalPort = 8080;

		private static readonly string LocalPortName = Sharpen.Extensions.ToString(LocalPort
			);

		/// <summary>
		/// Some slop around expected times when making sure timeouts behave
		/// as expected.
		/// </summary>
		/// <remarks>
		/// Some slop around expected times when making sure timeouts behave
		/// as expected. We assume that they will be accurate to within
		/// this threshold.
		/// </remarks>
		internal const long TimeFudgeMillis = 200;

		/// <summary>
		/// Test that we can't accidentally connect back to the connecting socket due
		/// to a quirk in the TCP spec.
		/// </summary>
		/// <remarks>
		/// Test that we can't accidentally connect back to the connecting socket due
		/// to a quirk in the TCP spec.
		/// This is a regression test for HADOOP-6722.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAvoidLoopbackTcpSockets()
		{
			Configuration conf = new Configuration();
			Socket socket = NetUtils.GetDefaultSocketFactory(conf).CreateSocket();
			socket.Bind2(new IPEndPoint("127.0.0.1", 0));
			System.Console.Error.WriteLine("local address: " + socket.GetLocalAddress());
			System.Console.Error.WriteLine("local port: " + socket.GetLocalPort());
			try
			{
				NetUtils.Connect(socket, new IPEndPoint(socket.GetLocalAddress(), socket.GetLocalPort
					()), 20000);
				socket.Close();
				NUnit.Framework.Assert.Fail("Should not have connected");
			}
			catch (ConnectException ce)
			{
				System.Console.Error.WriteLine("Got exception: " + ce);
				NUnit.Framework.Assert.IsTrue(ce.Message.Contains("resulted in a loopback"));
			}
			catch (SocketException se)
			{
				// Some TCP stacks will actually throw their own Invalid argument exception
				// here. This is also OK.
				NUnit.Framework.Assert.IsTrue(se.Message.Contains("Invalid argument"));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSocketReadTimeoutWithChannel()
		{
			DoSocketReadTimeoutTest(true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSocketReadTimeoutWithoutChannel()
		{
			DoSocketReadTimeoutTest(false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoSocketReadTimeoutTest(bool withChannel)
		{
			// Binding a ServerSocket is enough to accept connections.
			// Rely on the backlog to accept for us.
			Socket ss = Sharpen.Extensions.CreateServerSocket(0);
			Socket s;
			if (withChannel)
			{
				s = NetUtils.GetDefaultSocketFactory(new Configuration()).CreateSocket();
				Assume.AssumeNotNull(s.GetChannel());
			}
			else
			{
				s = new Socket();
				NUnit.Framework.Assert.IsNull(s.GetChannel());
			}
			SocketInputWrapper stm = null;
			try
			{
				NetUtils.Connect(s, ss.LocalEndPoint, 1000);
				stm = NetUtils.GetInputStream(s, 1000);
				AssertReadTimeout(stm, 1000);
				// Change timeout, make sure it applies.
				stm.SetTimeout(1);
				AssertReadTimeout(stm, 1);
				// If there is a channel, then setting the socket timeout
				// should not matter. If there is not a channel, it will
				// take effect.
				s.ReceiveTimeout = 1000;
				if (withChannel)
				{
					AssertReadTimeout(stm, 1);
				}
				else
				{
					AssertReadTimeout(stm, 1000);
				}
			}
			finally
			{
				IOUtils.CloseStream(stm);
				IOUtils.CloseSocket(s);
				ss.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertReadTimeout(SocketInputWrapper stm, int timeoutMillis)
		{
			long st = Runtime.NanoTime();
			try
			{
				stm.Read();
				NUnit.Framework.Assert.Fail("Didn't time out");
			}
			catch (SocketTimeoutException)
			{
				AssertTimeSince(st, timeoutMillis);
			}
		}

		private void AssertTimeSince(long startNanos, int expectedMillis)
		{
			long durationNano = Runtime.NanoTime() - startNanos;
			long millis = TimeUnit.Milliseconds.Convert(durationNano, TimeUnit.Nanoseconds);
			NUnit.Framework.Assert.IsTrue("Expected " + expectedMillis + "ms, but took " + millis
				, Math.Abs(millis - expectedMillis) < TimeFudgeMillis);
		}

		/// <summary>Test for {</summary>
		/// <exception cref="Sharpen.UnknownHostException">@link NetUtils#getLocalInetAddress(String)
		/// 	</exception>
		/// <exception cref="System.Net.Sockets.SocketException"></exception>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetLocalInetAddress()
		{
			NUnit.Framework.Assert.IsNotNull(NetUtils.GetLocalInetAddress("127.0.0.1"));
			NUnit.Framework.Assert.IsNull(NetUtils.GetLocalInetAddress("invalid-address-for-test"
				));
			NUnit.Framework.Assert.IsNull(NetUtils.GetLocalInetAddress(null));
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		public virtual void TestVerifyHostnamesException()
		{
			string[] names = new string[] { "valid.host.com", "1.com", "invalid host here" };
			NetUtils.VerifyHostnames(names);
		}

		[NUnit.Framework.Test]
		public virtual void TestVerifyHostnamesNoException()
		{
			string[] names = new string[] { "valid.host.com", "1.com" };
			try
			{
				NetUtils.VerifyHostnames(names);
			}
			catch (UnknownHostException)
			{
				NUnit.Framework.Assert.Fail("NetUtils.verifyHostnames threw unexpected UnknownHostException"
					);
			}
		}

		/// <summary>
		/// Test for
		/// <see cref="NetUtils.IsLocalAddress(System.Net.IPAddress)"/>
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIsLocalAddress()
		{
			// Test - local host is local address
			NUnit.Framework.Assert.IsTrue(NetUtils.IsLocalAddress(Sharpen.Runtime.GetLocalHost
				()));
			// Test - all addresses bound network interface is local address
			Enumeration<NetworkInterface> interfaces = NetworkInterface.GetNetworkInterfaces(
				);
			if (interfaces != null)
			{
				// Iterate through all network interfaces
				while (interfaces.MoveNext())
				{
					NetworkInterface i = interfaces.Current;
					Enumeration<IPAddress> addrs = i.GetInetAddresses();
					if (addrs == null)
					{
						continue;
					}
					// Iterate through all the addresses of a network interface
					while (addrs.MoveNext())
					{
						IPAddress addr = addrs.Current;
						NUnit.Framework.Assert.IsTrue(NetUtils.IsLocalAddress(addr));
					}
				}
			}
			NUnit.Framework.Assert.IsFalse(NetUtils.IsLocalAddress(Sharpen.Extensions.GetAddressByName
				("8.8.8.8")));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWrapConnectException()
		{
			IOException e = new ConnectException("failed");
			IOException wrapped = VerifyExceptionClass(e, typeof(ConnectException));
			AssertInException(wrapped, "failed");
			AssertWikified(wrapped);
			AssertInException(wrapped, "localhost");
			AssertRemoteDetailsIncluded(wrapped);
			AssertInException(wrapped, "/ConnectionRefused");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWrapBindException()
		{
			IOException e = new BindException("failed");
			IOException wrapped = VerifyExceptionClass(e, typeof(BindException));
			AssertInException(wrapped, "failed");
			AssertLocalDetailsIncluded(wrapped);
			AssertNotInException(wrapped, DestPortName);
			AssertInException(wrapped, "/BindException");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWrapUnknownHostException()
		{
			IOException e = new UnknownHostException("failed");
			IOException wrapped = VerifyExceptionClass(e, typeof(UnknownHostException));
			AssertInException(wrapped, "failed");
			AssertWikified(wrapped);
			AssertInException(wrapped, "localhost");
			AssertRemoteDetailsIncluded(wrapped);
			AssertInException(wrapped, "/UnknownHost");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWrapEOFException()
		{
			IOException e = new EOFException("eof");
			IOException wrapped = VerifyExceptionClass(e, typeof(EOFException));
			AssertInException(wrapped, "eof");
			AssertWikified(wrapped);
			AssertInException(wrapped, "localhost");
			AssertRemoteDetailsIncluded(wrapped);
			AssertInException(wrapped, "/EOFException");
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetConnectAddress()
		{
			NetUtils.AddStaticResolution("host", "127.0.0.1");
			IPEndPoint addr = NetUtils.CreateSocketAddrForHost("host", 1);
			IPEndPoint connectAddr = NetUtils.GetConnectAddress(addr);
			NUnit.Framework.Assert.AreEqual(addr.GetHostName(), connectAddr.GetHostName());
			addr = new IPEndPoint(1);
			connectAddr = NetUtils.GetConnectAddress(addr);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetLocalHost().GetHostName(), connectAddr
				.GetHostName());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateSocketAddress()
		{
			IPEndPoint addr = NetUtils.CreateSocketAddr("127.0.0.1:12345", 1000, "myconfig");
			NUnit.Framework.Assert.AreEqual("127.0.0.1", addr.Address.GetHostAddress());
			NUnit.Framework.Assert.AreEqual(12345, addr.Port);
			addr = NetUtils.CreateSocketAddr("127.0.0.1", 1000, "myconfig");
			NUnit.Framework.Assert.AreEqual("127.0.0.1", addr.Address.GetHostAddress());
			NUnit.Framework.Assert.AreEqual(1000, addr.Port);
			try
			{
				addr = NetUtils.CreateSocketAddr("127.0.0.1:blahblah", 1000, "myconfig");
				NUnit.Framework.Assert.Fail("Should have failed to parse bad port");
			}
			catch (ArgumentException iae)
			{
				AssertInException(iae, "myconfig");
			}
		}

		/// <exception cref="System.Exception"/>
		private void AssertRemoteDetailsIncluded(IOException wrapped)
		{
			AssertInException(wrapped, "desthost");
			AssertInException(wrapped, DestPortName);
		}

		/// <exception cref="System.Exception"/>
		private void AssertLocalDetailsIncluded(IOException wrapped)
		{
			AssertInException(wrapped, "localhost");
			AssertInException(wrapped, LocalPortName);
		}

		/// <exception cref="System.Exception"/>
		private void AssertWikified(Exception e)
		{
			AssertInException(e, NetUtils.HadoopWiki);
		}

		/// <exception cref="System.Exception"/>
		private void AssertInException(Exception e, string text)
		{
			string message = ExtractExceptionMessage(e);
			if (!(message.Contains(text)))
			{
				throw Sharpen.Extensions.InitCause(new AssertionFailedError("Wrong text in message "
					 + "\"" + message + "\"" + " expected \"" + text + "\""), e);
			}
		}

		/// <exception cref="System.Exception"/>
		private string ExtractExceptionMessage(Exception e)
		{
			NUnit.Framework.Assert.IsNotNull("Null Exception", e);
			string message = e.Message;
			if (message == null)
			{
				throw Sharpen.Extensions.InitCause(new AssertionFailedError("Empty text in exception "
					 + e), e);
			}
			return message;
		}

		/// <exception cref="System.Exception"/>
		private void AssertNotInException(Exception e, string text)
		{
			string message = ExtractExceptionMessage(e);
			if (message.Contains(text))
			{
				throw Sharpen.Extensions.InitCause(new AssertionFailedError("Wrong text in message "
					 + "\"" + message + "\"" + " did not expect \"" + text + "\""), e);
			}
		}

		/// <exception cref="System.Exception"/>
		private IOException VerifyExceptionClass(IOException e, Type expectedClass)
		{
			NUnit.Framework.Assert.IsNotNull("Null Exception", e);
			IOException wrapped = NetUtils.WrapException("desthost", DestPort, "localhost", LocalPort
				, e);
			Log.Info(wrapped.ToString(), wrapped);
			if (!(wrapped.GetType().Equals(expectedClass)))
			{
				throw Sharpen.Extensions.InitCause(new AssertionFailedError("Wrong exception class; expected "
					 + expectedClass + " got " + wrapped.GetType() + ": " + wrapped), wrapped);
			}
			return wrapped;
		}

		internal static NetUtilsTestResolver resolver;

		internal static Configuration config;

		[BeforeClass]
		public static void SetupResolver()
		{
			resolver = NetUtilsTestResolver.Install();
		}

		[SetUp]
		public virtual void ResetResolver()
		{
			resolver.Reset();
			config = new Configuration();
		}

		// getByExactName
		private void VerifyGetByExactNameSearch(string host, params string[] searches)
		{
			NUnit.Framework.Assert.IsNull(resolver.GetByExactName(host));
			AssertBetterArrayEquals(searches, resolver.GetHostSearches());
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByExactNameUnqualified()
		{
			VerifyGetByExactNameSearch("unknown", "unknown.");
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByExactNameUnqualifiedWithDomain()
		{
			VerifyGetByExactNameSearch("unknown.domain", "unknown.domain.");
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByExactNameQualified()
		{
			VerifyGetByExactNameSearch("unknown.", "unknown.");
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByExactNameQualifiedWithDomain()
		{
			VerifyGetByExactNameSearch("unknown.domain.", "unknown.domain.");
		}

		// getByNameWithSearch
		private void VerifyGetByNameWithSearch(string host, params string[] searches)
		{
			NUnit.Framework.Assert.IsNull(resolver.GetByNameWithSearch(host));
			AssertBetterArrayEquals(searches, resolver.GetHostSearches());
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByNameWithSearchUnqualified()
		{
			string host = "unknown";
			VerifyGetByNameWithSearch(host, host + ".a.b.", host + ".b.", host + ".c.");
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByNameWithSearchUnqualifiedWithDomain()
		{
			string host = "unknown.domain";
			VerifyGetByNameWithSearch(host, host + ".a.b.", host + ".b.", host + ".c.");
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByNameWithSearchQualified()
		{
			string host = "unknown.";
			VerifyGetByNameWithSearch(host, host);
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByNameWithSearchQualifiedWithDomain()
		{
			string host = "unknown.domain.";
			VerifyGetByNameWithSearch(host, host);
		}

		// getByName
		private void VerifyGetByName(string host, params string[] searches)
		{
			IPAddress addr = null;
			try
			{
				addr = resolver.GetByName(host);
			}
			catch (UnknownHostException)
			{
			}
			// ignore
			NUnit.Framework.Assert.IsNull(addr);
			AssertBetterArrayEquals(searches, resolver.GetHostSearches());
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByNameQualified()
		{
			string host = "unknown.";
			VerifyGetByName(host, host);
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByNameQualifiedWithDomain()
		{
			VerifyGetByName("unknown.domain.", "unknown.domain.");
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByNameUnqualified()
		{
			string host = "unknown";
			VerifyGetByName(host, host + ".a.b.", host + ".b.", host + ".c.", host + ".");
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverGetByNameUnqualifiedWithDomain()
		{
			string host = "unknown.domain";
			VerifyGetByName(host, host + ".", host + ".a.b.", host + ".b.", host + ".c.");
		}

		// resolving of hosts
		private IPAddress VerifyResolve(string host, params string[] searches)
		{
			IPAddress addr = null;
			try
			{
				addr = resolver.GetByName(host);
			}
			catch (UnknownHostException)
			{
			}
			// ignore
			NUnit.Framework.Assert.IsNotNull(addr);
			AssertBetterArrayEquals(searches, resolver.GetHostSearches());
			return addr;
		}

		private void VerifyInetAddress(IPAddress addr, string host, string ip)
		{
			NUnit.Framework.Assert.IsNotNull(addr);
			NUnit.Framework.Assert.AreEqual(host, addr.GetHostName());
			NUnit.Framework.Assert.AreEqual(ip, addr.GetHostAddress());
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverUnqualified()
		{
			string host = "host";
			IPAddress addr = VerifyResolve(host, host + ".a.b.");
			VerifyInetAddress(addr, "host.a.b", "1.1.1.1");
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverUnqualifiedWithDomain()
		{
			string host = "host.a";
			IPAddress addr = VerifyResolve(host, host + ".", host + ".a.b.", host + ".b.");
			VerifyInetAddress(addr, "host.a.b", "1.1.1.1");
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverUnqualifedFull()
		{
			string host = "host.a.b";
			IPAddress addr = VerifyResolve(host, host + ".");
			VerifyInetAddress(addr, host, "1.1.1.1");
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverQualifed()
		{
			string host = "host.a.b.";
			IPAddress addr = VerifyResolve(host, host);
			VerifyInetAddress(addr, host, "1.1.1.1");
		}

		// localhost
		[NUnit.Framework.Test]
		public virtual void TestResolverLoopback()
		{
			string host = "Localhost";
			IPAddress addr = VerifyResolve(host);
			// no lookup should occur
			VerifyInetAddress(addr, "Localhost", "127.0.0.1");
		}

		[NUnit.Framework.Test]
		public virtual void TestResolverIP()
		{
			string host = "1.1.1.1";
			IPAddress addr = VerifyResolve(host);
			// no lookup should occur for ips
			VerifyInetAddress(addr, host, host);
		}

		//
		[NUnit.Framework.Test]
		public virtual void TestCanonicalUriWithPort()
		{
			URI uri;
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme://host:123"), 456);
			NUnit.Framework.Assert.AreEqual("scheme://host.a.b:123", uri.ToString());
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme://host:123/"), 456);
			NUnit.Framework.Assert.AreEqual("scheme://host.a.b:123/", uri.ToString());
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme://host:123/path"), 456);
			NUnit.Framework.Assert.AreEqual("scheme://host.a.b:123/path", uri.ToString());
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme://host:123/path?q#frag"), 456);
			NUnit.Framework.Assert.AreEqual("scheme://host.a.b:123/path?q#frag", uri.ToString
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestCanonicalUriWithDefaultPort()
		{
			URI uri;
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme://host"), 123);
			NUnit.Framework.Assert.AreEqual("scheme://host.a.b:123", uri.ToString());
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme://host/"), 123);
			NUnit.Framework.Assert.AreEqual("scheme://host.a.b:123/", uri.ToString());
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme://host/path"), 123);
			NUnit.Framework.Assert.AreEqual("scheme://host.a.b:123/path", uri.ToString());
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme://host/path?q#frag"), 123);
			NUnit.Framework.Assert.AreEqual("scheme://host.a.b:123/path?q#frag", uri.ToString
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestCanonicalUriWithPath()
		{
			URI uri;
			uri = NetUtils.GetCanonicalUri(URI.Create("path"), 2);
			NUnit.Framework.Assert.AreEqual("path", uri.ToString());
			uri = NetUtils.GetCanonicalUri(URI.Create("/path"), 2);
			NUnit.Framework.Assert.AreEqual("/path", uri.ToString());
		}

		[NUnit.Framework.Test]
		public virtual void TestCanonicalUriWithNoAuthority()
		{
			URI uri;
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme:/"), 2);
			NUnit.Framework.Assert.AreEqual("scheme:/", uri.ToString());
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme:/path"), 2);
			NUnit.Framework.Assert.AreEqual("scheme:/path", uri.ToString());
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme:///"), 2);
			NUnit.Framework.Assert.AreEqual("scheme:///", uri.ToString());
			uri = NetUtils.GetCanonicalUri(URI.Create("scheme:///path"), 2);
			NUnit.Framework.Assert.AreEqual("scheme:///path", uri.ToString());
		}

		[NUnit.Framework.Test]
		public virtual void TestCanonicalUriWithNoHost()
		{
			URI uri = NetUtils.GetCanonicalUri(URI.Create("scheme://:123/path"), 2);
			NUnit.Framework.Assert.AreEqual("scheme://:123/path", uri.ToString());
		}

		[NUnit.Framework.Test]
		public virtual void TestCanonicalUriWithNoPortNoDefaultPort()
		{
			URI uri = NetUtils.GetCanonicalUri(URI.Create("scheme://host/path"), -1);
			NUnit.Framework.Assert.AreEqual("scheme://host.a.b/path", uri.ToString());
		}

		/// <summary>
		/// Test for
		/// <see cref="NetUtils.NormalizeHostNames(System.Collections.Generic.ICollection{E})
		/// 	"/>
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestNormalizeHostName()
		{
			IList<string> hosts = Arrays.AsList(new string[] { "127.0.0.1", "localhost", "1.kanyezone.appspot.com"
				, "UnknownHost123" });
			IList<string> normalizedHosts = NetUtils.NormalizeHostNames(hosts);
			// when ipaddress is normalized, same address is expected in return
			NUnit.Framework.Assert.AreEqual(normalizedHosts[0], hosts[0]);
			// for normalizing a resolvable hostname, resolved ipaddress is expected in return
			NUnit.Framework.Assert.IsFalse(normalizedHosts[1].Equals(hosts[1]));
			NUnit.Framework.Assert.AreEqual(normalizedHosts[1], hosts[0]);
			// this address HADOOP-8372: when normalizing a valid resolvable hostname start with numeric, 
			// its ipaddress is expected to return
			NUnit.Framework.Assert.IsFalse(normalizedHosts[2].Equals(hosts[2]));
			// return the same hostname after normalizing a irresolvable hostname.
			NUnit.Framework.Assert.AreEqual(normalizedHosts[3], hosts[3]);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetHostNameOfIP()
		{
			NUnit.Framework.Assert.IsNull(NetUtils.GetHostNameOfIP(null));
			NUnit.Framework.Assert.IsNull(NetUtils.GetHostNameOfIP(string.Empty));
			NUnit.Framework.Assert.IsNull(NetUtils.GetHostNameOfIP("crazytown"));
			NUnit.Framework.Assert.IsNull(NetUtils.GetHostNameOfIP("127.0.0.1:"));
			// no port
			NUnit.Framework.Assert.IsNull(NetUtils.GetHostNameOfIP("127.0.0.1:-1"));
			// bogus port
			NUnit.Framework.Assert.IsNull(NetUtils.GetHostNameOfIP("127.0.0.1:A"));
			// bogus port
			NUnit.Framework.Assert.IsNotNull(NetUtils.GetHostNameOfIP("127.0.0.1"));
			NUnit.Framework.Assert.IsNotNull(NetUtils.GetHostNameOfIP("127.0.0.1:1"));
		}

		[NUnit.Framework.Test]
		public virtual void TestTrimCreateSocketAddress()
		{
			Configuration conf = new Configuration();
			NetUtils.AddStaticResolution("host", "127.0.0.1");
			string defaultAddr = "host:1  ";
			IPEndPoint addr = NetUtils.CreateSocketAddr(defaultAddr);
			conf.SetSocketAddr("myAddress", addr);
			NUnit.Framework.Assert.AreEqual(defaultAddr.Trim(), NetUtils.GetHostPortString(addr
				));
		}

		private void AssertBetterArrayEquals<T>(T[] expect, T[] got)
		{
			string expectStr = StringUtils.Join(expect, ", ");
			string gotStr = StringUtils.Join(got, ", ");
			NUnit.Framework.Assert.AreEqual(expectStr, gotStr);
		}
	}
}
