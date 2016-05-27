using System;
using System.IO;
using System.Net;
using Javax.Security.Auth.Kerberos;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class TestSecurityUtil
	{
		[BeforeClass]
		public static void UnsetKerberosRealm()
		{
			// prevent failures if kinit-ed or on os x with no realm
			Runtime.SetProperty("java.security.krb5.kdc", string.Empty);
			Runtime.SetProperty("java.security.krb5.realm", "NONE");
		}

		[NUnit.Framework.Test]
		public virtual void IsOriginalTGTReturnsCorrectValues()
		{
			NUnit.Framework.Assert.IsTrue(SecurityUtil.IsTGSPrincipal(new KerberosPrincipal("krbtgt/foo@foo"
				)));
			NUnit.Framework.Assert.IsTrue(SecurityUtil.IsTGSPrincipal(new KerberosPrincipal("krbtgt/foo.bar.bat@foo.bar.bat"
				)));
			NUnit.Framework.Assert.IsFalse(SecurityUtil.IsTGSPrincipal(null));
			NUnit.Framework.Assert.IsFalse(SecurityUtil.IsTGSPrincipal(new KerberosPrincipal(
				"blah")));
			NUnit.Framework.Assert.IsFalse(SecurityUtil.IsTGSPrincipal(new KerberosPrincipal(
				"krbtgt/hello")));
			NUnit.Framework.Assert.IsFalse(SecurityUtil.IsTGSPrincipal(new KerberosPrincipal(
				"krbtgt/foo@FOO")));
		}

		/// <exception cref="System.IO.IOException"/>
		private void Verify(string original, string hostname, string expected)
		{
			NUnit.Framework.Assert.AreEqual(expected, SecurityUtil.GetServerPrincipal(original
				, hostname));
			IPAddress addr = MockAddr(hostname);
			NUnit.Framework.Assert.AreEqual(expected, SecurityUtil.GetServerPrincipal(original
				, addr));
		}

		private IPAddress MockAddr(string reverseTo)
		{
			IPAddress mock = Org.Mockito.Mockito.Mock<IPAddress>();
			Org.Mockito.Mockito.DoReturn(reverseTo).When(mock).ToString();
			return mock;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetServerPrincipal()
		{
			string service = "hdfs/";
			string realm = "@REALM";
			string hostname = "foohost";
			string userPrincipal = "foo@FOOREALM";
			string shouldReplace = service + SecurityUtil.HostnamePattern + realm;
			string replaced = service + hostname + realm;
			Verify(shouldReplace, hostname, replaced);
			string shouldNotReplace = service + SecurityUtil.HostnamePattern + "NAME" + realm;
			Verify(shouldNotReplace, hostname, shouldNotReplace);
			Verify(userPrincipal, hostname, userPrincipal);
			// testing reverse DNS lookup doesn't happen
			IPAddress notUsed = Org.Mockito.Mockito.Mock<IPAddress>();
			NUnit.Framework.Assert.AreEqual(shouldNotReplace, SecurityUtil.GetServerPrincipal
				(shouldNotReplace, notUsed));
			Org.Mockito.Mockito.Verify(notUsed, Org.Mockito.Mockito.Never()).ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPrincipalsWithLowerCaseHosts()
		{
			string service = "xyz/";
			string realm = "@REALM";
			string principalInConf = service + SecurityUtil.HostnamePattern + realm;
			string hostname = "FooHost";
			string principal = service + StringUtils.ToLowerCase(hostname) + realm;
			Verify(principalInConf, hostname, principal);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalHostNameForNullOrWild()
		{
			string local = StringUtils.ToLowerCase(SecurityUtil.GetLocalHostName());
			NUnit.Framework.Assert.AreEqual("hdfs/" + local + "@REALM", SecurityUtil.GetServerPrincipal
				("hdfs/_HOST@REALM", (string)null));
			NUnit.Framework.Assert.AreEqual("hdfs/" + local + "@REALM", SecurityUtil.GetServerPrincipal
				("hdfs/_HOST@REALM", "0.0.0.0"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStartsWithIncorrectSettings()
		{
			Configuration conf = new Configuration();
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, conf);
			string keyTabKey = "key";
			conf.Set(keyTabKey, string.Empty);
			UserGroupInformation.SetConfiguration(conf);
			bool gotException = false;
			try
			{
				SecurityUtil.Login(conf, keyTabKey, string.Empty, string.Empty);
			}
			catch (IOException)
			{
				// expected
				gotException = true;
			}
			NUnit.Framework.Assert.IsTrue("Exception for empty keytabfile name was expected", 
				gotException);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetHostFromPrincipal()
		{
			NUnit.Framework.Assert.AreEqual("host", SecurityUtil.GetHostFromPrincipal("service/host@realm"
				));
			NUnit.Framework.Assert.AreEqual(null, SecurityUtil.GetHostFromPrincipal("service@realm"
				));
		}

		[NUnit.Framework.Test]
		public virtual void TestBuildDTServiceName()
		{
			SecurityUtil.SetTokenServiceUseIp(true);
			NUnit.Framework.Assert.AreEqual("127.0.0.1:123", SecurityUtil.BuildDTServiceName(
				URI.Create("test://LocalHost"), 123));
			NUnit.Framework.Assert.AreEqual("127.0.0.1:123", SecurityUtil.BuildDTServiceName(
				URI.Create("test://LocalHost:123"), 456));
			NUnit.Framework.Assert.AreEqual("127.0.0.1:123", SecurityUtil.BuildDTServiceName(
				URI.Create("test://127.0.0.1"), 123));
			NUnit.Framework.Assert.AreEqual("127.0.0.1:123", SecurityUtil.BuildDTServiceName(
				URI.Create("test://127.0.0.1:123"), 456));
		}

		[NUnit.Framework.Test]
		public virtual void TestBuildTokenServiceSockAddr()
		{
			SecurityUtil.SetTokenServiceUseIp(true);
			NUnit.Framework.Assert.AreEqual("127.0.0.1:123", SecurityUtil.BuildTokenService(new 
				IPEndPoint("LocalHost", 123)).ToString());
			NUnit.Framework.Assert.AreEqual("127.0.0.1:123", SecurityUtil.BuildTokenService(new 
				IPEndPoint("127.0.0.1", 123)).ToString());
			// what goes in, comes out
			NUnit.Framework.Assert.AreEqual("127.0.0.1:123", SecurityUtil.BuildTokenService(NetUtils
				.CreateSocketAddr("127.0.0.1", 123)).ToString());
		}

		[NUnit.Framework.Test]
		public virtual void TestGoodHostsAndPorts()
		{
			IPEndPoint compare = NetUtils.CreateSocketAddrForHost("localhost", 123);
			RunGoodCases(compare, "localhost", 123);
			RunGoodCases(compare, "localhost:", 123);
			RunGoodCases(compare, "localhost:123", 456);
		}

		internal virtual void RunGoodCases(IPEndPoint addr, string host, int port)
		{
			NUnit.Framework.Assert.AreEqual(addr, NetUtils.CreateSocketAddr(host, port));
			NUnit.Framework.Assert.AreEqual(addr, NetUtils.CreateSocketAddr("hdfs://" + host, 
				port));
			NUnit.Framework.Assert.AreEqual(addr, NetUtils.CreateSocketAddr("hdfs://" + host 
				+ "/path", port));
		}

		[NUnit.Framework.Test]
		public virtual void TestBadHostsAndPorts()
		{
			RunBadCases(string.Empty, true);
			RunBadCases(":", false);
			RunBadCases("hdfs/", false);
			RunBadCases("hdfs:/", false);
			RunBadCases("hdfs://", true);
		}

		internal virtual void RunBadCases(string prefix, bool validIfPosPort)
		{
			RunBadPortPermutes(prefix, false);
			RunBadPortPermutes(prefix + "*", false);
			RunBadPortPermutes(prefix + "localhost", validIfPosPort);
			RunBadPortPermutes(prefix + "localhost:-1", false);
			RunBadPortPermutes(prefix + "localhost:-123", false);
			RunBadPortPermutes(prefix + "localhost:xyz", false);
			RunBadPortPermutes(prefix + "localhost/xyz", validIfPosPort);
			RunBadPortPermutes(prefix + "localhost/:123", validIfPosPort);
			RunBadPortPermutes(prefix + ":123", false);
			RunBadPortPermutes(prefix + ":xyz", false);
		}

		internal virtual void RunBadPortPermutes(string arg, bool validIfPosPort)
		{
			int[] ports = new int[] { -123, -1, 123 };
			bool bad = false;
			try
			{
				NetUtils.CreateSocketAddr(arg);
			}
			catch (ArgumentException)
			{
				bad = true;
			}
			finally
			{
				NUnit.Framework.Assert.IsTrue("should be bad: '" + arg + "'", bad);
			}
			foreach (int port in ports)
			{
				if (validIfPosPort && port > 0)
				{
					continue;
				}
				bad = false;
				try
				{
					NetUtils.CreateSocketAddr(arg, port);
				}
				catch (ArgumentException)
				{
					bad = true;
				}
				finally
				{
					NUnit.Framework.Assert.IsTrue("should be bad: '" + arg + "' (default port:" + port
						 + ")", bad);
				}
			}
		}

		// check that the socket addr has:
		// 1) the InetSocketAddress has the correct hostname, ie. exact host/ip given
		// 2) the address is resolved, ie. has an ip
		// 3,4) the socket's InetAddress has the same hostname, and the correct ip
		// 5) the port is correct
		private void VerifyValues(IPEndPoint addr, string host, string ip, int port)
		{
			NUnit.Framework.Assert.IsTrue(!addr.IsUnresolved());
			// don't know what the standard resolver will return for hostname.
			// should be host for host; host or ip for ip is ambiguous
			if (!SecurityUtil.useIpForTokenService)
			{
				NUnit.Framework.Assert.AreEqual(host, addr.GetHostName());
				NUnit.Framework.Assert.AreEqual(host, addr.Address.GetHostName());
			}
			NUnit.Framework.Assert.AreEqual(ip, addr.Address.GetHostAddress());
			NUnit.Framework.Assert.AreEqual(port, addr.Port);
		}

		// check:
		// 1) buildTokenService honors use_ip setting
		// 2) setTokenService & getService works
		// 3) getTokenServiceAddr decodes to the identical socket addr
		private void VerifyTokenService(IPEndPoint addr, string host, string ip, int port
			, bool useIp)
		{
			//LOG.info("address:"+addr+" host:"+host+" ip:"+ip+" port:"+port);
			SecurityUtil.SetTokenServiceUseIp(useIp);
			string serviceHost = useIp ? ip : StringUtils.ToLowerCase(host);
			Org.Apache.Hadoop.Security.Token.Token<object> token = new Org.Apache.Hadoop.Security.Token.Token
				<TokenIdentifier>();
			Text service = new Text(serviceHost + ":" + port);
			NUnit.Framework.Assert.AreEqual(service, SecurityUtil.BuildTokenService(addr));
			SecurityUtil.SetTokenService(token, addr);
			NUnit.Framework.Assert.AreEqual(service, token.GetService());
			IPEndPoint serviceAddr = SecurityUtil.GetTokenServiceAddr(token);
			NUnit.Framework.Assert.IsNotNull(serviceAddr);
			VerifyValues(serviceAddr, serviceHost, ip, port);
		}

		// check:
		// 1) socket addr is created with fields set as expected
		// 2) token service with ips
		// 3) token service with the given host or ip
		private void VerifyAddress(IPEndPoint addr, string host, string ip, int port)
		{
			VerifyValues(addr, host, ip, port);
			//LOG.info("test that token service uses ip");
			VerifyTokenService(addr, host, ip, port, true);
			//LOG.info("test that token service uses host");
			VerifyTokenService(addr, host, ip, port, false);
		}

		// check:
		// 1-4) combinations of host and port
		// this will construct a socket addr, verify all the fields, build the
		// service to verify the use_ip setting is honored, set the token service
		// based on addr and verify the token service is set correctly, decode
		// the token service and ensure all the fields of the decoded addr match
		private void VerifyServiceAddr(string host, string ip)
		{
			IPEndPoint addr;
			int port = 123;
			// test host, port tuple
			//LOG.info("test tuple ("+host+","+port+")");
			addr = NetUtils.CreateSocketAddrForHost(host, port);
			VerifyAddress(addr, host, ip, port);
			// test authority with no default port
			//LOG.info("test authority '"+host+":"+port+"'");
			addr = NetUtils.CreateSocketAddr(host + ":" + port);
			VerifyAddress(addr, host, ip, port);
			// test authority with a default port, make sure default isn't used
			//LOG.info("test authority '"+host+":"+port+"' with ignored default port");
			addr = NetUtils.CreateSocketAddr(host + ":" + port, port + 1);
			VerifyAddress(addr, host, ip, port);
			// test host-only authority, using port as default port
			//LOG.info("test host:"+host+" port:"+port);
			addr = NetUtils.CreateSocketAddr(host, port);
			VerifyAddress(addr, host, ip, port);
		}

		[NUnit.Framework.Test]
		public virtual void TestSocketAddrWithName()
		{
			string staticHost = "my";
			NetUtils.AddStaticResolution(staticHost, "localhost");
			VerifyServiceAddr("LocalHost", "127.0.0.1");
		}

		[NUnit.Framework.Test]
		public virtual void TestSocketAddrWithIP()
		{
			string staticHost = "127.0.0.1";
			NetUtils.AddStaticResolution(staticHost, "localhost");
			VerifyServiceAddr(staticHost, "127.0.0.1");
		}

		[NUnit.Framework.Test]
		public virtual void TestSocketAddrWithNameToStaticName()
		{
			string staticHost = "host1";
			NetUtils.AddStaticResolution(staticHost, "localhost");
			VerifyServiceAddr(staticHost, "127.0.0.1");
		}

		[NUnit.Framework.Test]
		public virtual void TestSocketAddrWithNameToStaticIP()
		{
			string staticHost = "host3";
			NetUtils.AddStaticResolution(staticHost, "255.255.255.255");
			VerifyServiceAddr(staticHost, "255.255.255.255");
		}

		// this is a bizarre case, but it's if a test tries to remap an ip address
		[NUnit.Framework.Test]
		public virtual void TestSocketAddrWithIPToStaticIP()
		{
			string staticHost = "1.2.3.4";
			NetUtils.AddStaticResolution(staticHost, "255.255.255.255");
			VerifyServiceAddr(staticHost, "255.255.255.255");
		}

		[NUnit.Framework.Test]
		public virtual void TestGetAuthenticationMethod()
		{
			Configuration conf = new Configuration();
			// default is simple
			conf.Unset(CommonConfigurationKeysPublic.HadoopSecurityAuthentication);
			NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Simple, 
				SecurityUtil.GetAuthenticationMethod(conf));
			// simple
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "simple");
			NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Simple, 
				SecurityUtil.GetAuthenticationMethod(conf));
			// kerberos
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Kerberos
				, SecurityUtil.GetAuthenticationMethod(conf));
			// bad value
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kaboom");
			string error = null;
			try
			{
				SecurityUtil.GetAuthenticationMethod(conf);
			}
			catch (Exception e)
			{
				error = e.ToString();
			}
			NUnit.Framework.Assert.AreEqual("java.lang.IllegalArgumentException: " + "Invalid attribute value for "
				 + CommonConfigurationKeysPublic.HadoopSecurityAuthentication + " of kaboom", error
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestSetAuthenticationMethod()
		{
			Configuration conf = new Configuration();
			// default
			SecurityUtil.SetAuthenticationMethod(null, conf);
			NUnit.Framework.Assert.AreEqual("simple", conf.Get(CommonConfigurationKeysPublic.
				HadoopSecurityAuthentication));
			// simple
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Simple
				, conf);
			NUnit.Framework.Assert.AreEqual("simple", conf.Get(CommonConfigurationKeysPublic.
				HadoopSecurityAuthentication));
			// kerberos
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, conf);
			NUnit.Framework.Assert.AreEqual("kerberos", conf.Get(CommonConfigurationKeysPublic
				.HadoopSecurityAuthentication));
		}
	}
}
