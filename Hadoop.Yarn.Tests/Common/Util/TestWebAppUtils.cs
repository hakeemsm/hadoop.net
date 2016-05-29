using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class TestWebAppUtils
	{
		private const string Rm1NodeId = "rm1";

		private const string Rm2NodeId = "rm2";

		private static string[] dummyHostNames = new string[] { "host1", "host2", "host3"
			 };

		private const string anyIpAddress = "1.2.3.4";

		private static IDictionary<string, string> savedStaticResolution = new Dictionary
			<string, string>();

		// Because WebAppUtils#getResolvedAddress tries to resolve the hostname, we add a static mapping for dummy hostnames
		// to make this test run anywhere without having to give some resolvable hostnames
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void InitializeDummyHostnameResolution()
		{
			string previousIpAddress;
			foreach (string hostName in dummyHostNames)
			{
				if (null != (previousIpAddress = NetUtils.GetStaticResolution(hostName)))
				{
					savedStaticResolution[hostName] = previousIpAddress;
				}
				NetUtils.AddStaticResolution(hostName, anyIpAddress);
			}
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void RestoreDummyHostnameResolution()
		{
			foreach (KeyValuePair<string, string> hostnameToIpEntry in savedStaticResolution)
			{
				NetUtils.AddStaticResolution(hostnameToIpEntry.Key, hostnameToIpEntry.Value);
			}
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		[NUnit.Framework.Test]
		public virtual void TestRMWebAppURLRemoteAndLocal()
		{
			Configuration configuration = new Configuration();
			string rmAddress = "host1:8088";
			configuration.Set(YarnConfiguration.RmWebappAddress, rmAddress);
			string rm1Address = "host2:8088";
			string rm2Address = "host3:8088";
			configuration.Set(YarnConfiguration.RmWebappAddress + "." + Rm1NodeId, rm1Address
				);
			configuration.Set(YarnConfiguration.RmWebappAddress + "." + Rm2NodeId, rm2Address
				);
			configuration.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			configuration.Set(YarnConfiguration.RmHaIds, Rm1NodeId + "," + Rm2NodeId);
			string rmRemoteUrl = WebAppUtils.GetResolvedRemoteRMWebAppURLWithoutScheme(configuration
				);
			NUnit.Framework.Assert.AreEqual("ResolvedRemoteRMWebAppUrl should resolve to the first HA RM address"
				, rm1Address, rmRemoteUrl);
			string rmLocalUrl = WebAppUtils.GetResolvedRMWebAppURLWithoutScheme(configuration
				);
			NUnit.Framework.Assert.AreEqual("ResolvedRMWebAppUrl should resolve to the default RM webapp address"
				, rmAddress, rmLocalUrl);
		}
	}
}
