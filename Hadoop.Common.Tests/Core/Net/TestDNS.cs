using System;
using System.Net;
using Javax.Naming;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	/// <summary>Test host name and IP resolution and caching.</summary>
	public class TestDNS
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDNS));

		private const string Default = "default";

		/// <summary>Test that asking for the default hostname works</summary>
		/// <exception cref="System.Exception">if hostname lookups fail</exception>
		[Fact]
		public virtual void TestGetLocalHost()
		{
			string hostname = DNS.GetDefaultHost(Default);
			NUnit.Framework.Assert.IsNotNull(hostname);
		}

		/// <summary>
		/// Test that repeated calls to getting the local host are fairly fast, and
		/// hence that caching is being used
		/// </summary>
		/// <exception cref="System.Exception">if hostname lookups fail</exception>
		[Fact]
		public virtual void TestGetLocalHostIsFast()
		{
			string hostname1 = DNS.GetDefaultHost(Default);
			NUnit.Framework.Assert.IsNotNull(hostname1);
			string hostname2 = DNS.GetDefaultHost(Default);
			long t1 = Time.Now();
			string hostname3 = DNS.GetDefaultHost(Default);
			long t2 = Time.Now();
			Assert.Equal(hostname3, hostname2);
			Assert.Equal(hostname2, hostname1);
			long interval = t2 - t1;
			Assert.True("Took too long to determine local host - caching is not working"
				, interval < 20000);
		}

		/// <summary>Test that our local IP address is not null</summary>
		/// <exception cref="System.Exception">if something went wrong</exception>
		[Fact]
		public virtual void TestLocalHostHasAnAddress()
		{
			NUnit.Framework.Assert.IsNotNull(GetLocalIPAddr());
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		private IPAddress GetLocalIPAddr()
		{
			string hostname = DNS.GetDefaultHost(Default);
			IPAddress localhost = Sharpen.Extensions.GetAddressByName(hostname);
			return localhost;
		}

		/// <summary>Test null interface name</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNullInterface()
		{
			try
			{
				string host = DNS.GetDefaultHost(null);
				NUnit.Framework.Assert.Fail("Expected a NullPointerException, got " + host);
			}
			catch (ArgumentNullException)
			{
			}
			// Expected
			try
			{
				string ip = DNS.GetDefaultIP(null);
				NUnit.Framework.Assert.Fail("Expected a NullPointerException, got " + ip);
			}
			catch (ArgumentNullException)
			{
			}
		}

		// Expected
		/// <summary>Get the IP addresses of an unknown interface</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestIPsOfUnknownInterface()
		{
			try
			{
				DNS.GetIPs("name-of-an-unknown-interface");
				NUnit.Framework.Assert.Fail("Got an IP for a bogus interface");
			}
			catch (UnknownHostException e)
			{
				Assert.Equal("No such interface name-of-an-unknown-interface", 
					e.Message);
			}
		}

		/// <summary>Test the "default" IP addresses is the local IP addr</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetIPWithDefault()
		{
			string[] ips = DNS.GetIPs(Default);
			Assert.Equal("Should only return 1 default IP", 1, ips.Length);
			Assert.Equal(GetLocalIPAddr().GetHostAddress(), ips[0].ToString
				());
			string ip = DNS.GetDefaultIP(Default);
			Assert.Equal(ip, ips[0].ToString());
		}

		/// <summary>TestCase: get our local address and reverse look it up</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRDNS()
		{
			IPAddress localhost = GetLocalIPAddr();
			try
			{
				string s = DNS.ReverseDns(localhost, null);
				Log.Info("Local revers DNS hostname is " + s);
			}
			catch (NameNotFoundException e)
			{
				if (!localhost.IsLinkLocalAddress() || localhost.IsLoopbackAddress())
				{
					//these addresses probably won't work with rDNS anyway, unless someone
					//has unusual entries in their DNS server mapping 1.0.0.127 to localhost
					Log.Info("Reverse DNS failing as due to incomplete networking", e);
					Log.Info("Address is " + localhost + " Loopback=" + localhost.IsLoopbackAddress()
						 + " Linklocal=" + localhost.IsLinkLocalAddress());
				}
			}
		}

		/// <summary>Test that the name "localhost" resolves to something.</summary>
		/// <remarks>
		/// Test that the name "localhost" resolves to something.
		/// If this fails, your machine's network is in a mess, go edit /etc/hosts
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLocalhostResolves()
		{
			IPAddress localhost = Sharpen.Extensions.GetAddressByName("localhost");
			NUnit.Framework.Assert.IsNotNull("localhost is null", localhost);
			Log.Info("Localhost IPAddr is " + localhost.ToString());
		}
	}
}
