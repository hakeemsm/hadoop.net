using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy.Amfilter
{
	public class TestAmFilterInitializer : TestCase
	{
		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			base.SetUp();
			NetUtils.AddStaticResolution("host1", "172.0.0.1");
			NetUtils.AddStaticResolution("host2", "172.0.0.1");
			NetUtils.AddStaticResolution("host3", "172.0.0.1");
			NetUtils.AddStaticResolution("host4", "172.0.0.1");
			NetUtils.AddStaticResolution("host5", "172.0.0.1");
			NetUtils.AddStaticResolution("host6", "172.0.0.1");
		}

		[NUnit.Framework.Test]
		public virtual void TestInitFilter()
		{
			// Check PROXY_ADDRESS
			TestAmFilterInitializer.MockFilterContainer con = new TestAmFilterInitializer.MockFilterContainer
				(this);
			Configuration conf = new Configuration(false);
			conf.Set(YarnConfiguration.ProxyAddress, "host1:1000");
			AmFilterInitializer afi = new TestAmFilterInitializer.MockAmFilterInitializer(this
				);
			NUnit.Framework.Assert.IsNull(con.givenParameters);
			afi.InitFilter(con, conf);
			NUnit.Framework.Assert.AreEqual(2, con.givenParameters.Count);
			NUnit.Framework.Assert.AreEqual("host1", con.givenParameters[AmIpFilter.ProxyHosts
				]);
			NUnit.Framework.Assert.AreEqual("http://host1:1000/foo", con.givenParameters[AmIpFilter
				.ProxyUriBases]);
			// Check a single RM_WEBAPP_ADDRESS
			con = new TestAmFilterInitializer.MockFilterContainer(this);
			conf = new Configuration(false);
			conf.Set(YarnConfiguration.RmWebappAddress, "host2:2000");
			afi = new TestAmFilterInitializer.MockAmFilterInitializer(this);
			NUnit.Framework.Assert.IsNull(con.givenParameters);
			afi.InitFilter(con, conf);
			NUnit.Framework.Assert.AreEqual(2, con.givenParameters.Count);
			NUnit.Framework.Assert.AreEqual("host2", con.givenParameters[AmIpFilter.ProxyHosts
				]);
			NUnit.Framework.Assert.AreEqual("http://host2:2000/foo", con.givenParameters[AmIpFilter
				.ProxyUriBases]);
			// Check multiple RM_WEBAPP_ADDRESSes (RM HA)
			con = new TestAmFilterInitializer.MockFilterContainer(this);
			conf = new Configuration(false);
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.Set(YarnConfiguration.RmHaIds, "rm1,rm2,rm3");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm1", "host2:2000");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm2", "host3:3000");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm3", "host4:4000");
			afi = new TestAmFilterInitializer.MockAmFilterInitializer(this);
			NUnit.Framework.Assert.IsNull(con.givenParameters);
			afi.InitFilter(con, conf);
			NUnit.Framework.Assert.AreEqual(2, con.givenParameters.Count);
			string[] proxyHosts = con.givenParameters[AmIpFilter.ProxyHosts].Split(AmIpFilter
				.ProxyHostsDelimiter);
			NUnit.Framework.Assert.AreEqual(3, proxyHosts.Length);
			Arrays.Sort(proxyHosts);
			NUnit.Framework.Assert.AreEqual("host2", proxyHosts[0]);
			NUnit.Framework.Assert.AreEqual("host3", proxyHosts[1]);
			NUnit.Framework.Assert.AreEqual("host4", proxyHosts[2]);
			string[] proxyBases = con.givenParameters[AmIpFilter.ProxyUriBases].Split(AmIpFilter
				.ProxyUriBasesDelimiter);
			NUnit.Framework.Assert.AreEqual(3, proxyBases.Length);
			Arrays.Sort(proxyBases);
			NUnit.Framework.Assert.AreEqual("http://host2:2000/foo", proxyBases[0]);
			NUnit.Framework.Assert.AreEqual("http://host3:3000/foo", proxyBases[1]);
			NUnit.Framework.Assert.AreEqual("http://host4:4000/foo", proxyBases[2]);
			// Check multiple RM_WEBAPP_ADDRESSes (RM HA) with HTTPS
			con = new TestAmFilterInitializer.MockFilterContainer(this);
			conf = new Configuration(false);
			conf.Set(YarnConfiguration.YarnHttpPolicyKey, HttpConfig.Policy.HttpsOnly.ToString
				());
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.Set(YarnConfiguration.RmHaIds, "rm1,rm2");
			conf.Set(YarnConfiguration.RmWebappHttpsAddress + ".rm1", "host5:5000");
			conf.Set(YarnConfiguration.RmWebappHttpsAddress + ".rm2", "host6:6000");
			afi = new TestAmFilterInitializer.MockAmFilterInitializer(this);
			NUnit.Framework.Assert.IsNull(con.givenParameters);
			afi.InitFilter(con, conf);
			NUnit.Framework.Assert.AreEqual(2, con.givenParameters.Count);
			proxyHosts = con.givenParameters[AmIpFilter.ProxyHosts].Split(AmIpFilter.ProxyHostsDelimiter
				);
			NUnit.Framework.Assert.AreEqual(2, proxyHosts.Length);
			Arrays.Sort(proxyHosts);
			NUnit.Framework.Assert.AreEqual("host5", proxyHosts[0]);
			NUnit.Framework.Assert.AreEqual("host6", proxyHosts[1]);
			proxyBases = con.givenParameters[AmIpFilter.ProxyUriBases].Split(AmIpFilter.ProxyUriBasesDelimiter
				);
			NUnit.Framework.Assert.AreEqual(2, proxyBases.Length);
			Arrays.Sort(proxyBases);
			NUnit.Framework.Assert.AreEqual("https://host5:5000/foo", proxyBases[0]);
			NUnit.Framework.Assert.AreEqual("https://host6:6000/foo", proxyBases[1]);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetProxyHostsAndPortsForAmFilter()
		{
			// Check no configs given
			Configuration conf = new Configuration(false);
			IList<string> proxyHosts = WebAppUtils.GetProxyHostsAndPortsForAmFilter(conf);
			NUnit.Framework.Assert.AreEqual(1, proxyHosts.Count);
			NUnit.Framework.Assert.AreEqual(WebAppUtils.GetResolvedRMWebAppURLWithoutScheme(conf
				), proxyHosts[0]);
			// Check PROXY_ADDRESS has priority
			conf = new Configuration(false);
			conf.Set(YarnConfiguration.ProxyAddress, "host1:1000");
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.Set(YarnConfiguration.RmHaIds, "rm1,rm2,rm3");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm1", "host2:2000");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm2", "host3:3000");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm3", "host4:4000");
			proxyHosts = WebAppUtils.GetProxyHostsAndPortsForAmFilter(conf);
			NUnit.Framework.Assert.AreEqual(1, proxyHosts.Count);
			NUnit.Framework.Assert.AreEqual("host1:1000", proxyHosts[0]);
			// Check getting a single RM_WEBAPP_ADDRESS
			conf = new Configuration(false);
			conf.Set(YarnConfiguration.RmWebappAddress, "host2:2000");
			proxyHosts = WebAppUtils.GetProxyHostsAndPortsForAmFilter(conf);
			NUnit.Framework.Assert.AreEqual(1, proxyHosts.Count);
			proxyHosts.Sort();
			NUnit.Framework.Assert.AreEqual("host2:2000", proxyHosts[0]);
			// Check getting multiple RM_WEBAPP_ADDRESSes (RM HA)
			conf = new Configuration(false);
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.Set(YarnConfiguration.RmHaIds, "rm1,rm2,rm3");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm1", "host2:2000");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm2", "host3:3000");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm3", "host4:4000");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm4", "dummy");
			conf.Set(YarnConfiguration.RmWebappHttpsAddress + ".rm1", "host5:5000");
			conf.Set(YarnConfiguration.RmWebappHttpsAddress + ".rm2", "host6:6000");
			proxyHosts = WebAppUtils.GetProxyHostsAndPortsForAmFilter(conf);
			NUnit.Framework.Assert.AreEqual(3, proxyHosts.Count);
			proxyHosts.Sort();
			NUnit.Framework.Assert.AreEqual("host2:2000", proxyHosts[0]);
			NUnit.Framework.Assert.AreEqual("host3:3000", proxyHosts[1]);
			NUnit.Framework.Assert.AreEqual("host4:4000", proxyHosts[2]);
			// Check getting multiple RM_WEBAPP_ADDRESSes (RM HA) with HTTPS
			conf = new Configuration(false);
			conf.Set(YarnConfiguration.YarnHttpPolicyKey, HttpConfig.Policy.HttpsOnly.ToString
				());
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.Set(YarnConfiguration.RmHaIds, "rm1,rm2,rm3,dummy");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm1", "host2:2000");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm2", "host3:3000");
			conf.Set(YarnConfiguration.RmWebappAddress + ".rm3", "host4:4000");
			conf.Set(YarnConfiguration.RmWebappHttpsAddress + ".rm1", "host5:5000");
			conf.Set(YarnConfiguration.RmWebappHttpsAddress + ".rm2", "host6:6000");
			proxyHosts = WebAppUtils.GetProxyHostsAndPortsForAmFilter(conf);
			NUnit.Framework.Assert.AreEqual(2, proxyHosts.Count);
			proxyHosts.Sort();
			NUnit.Framework.Assert.AreEqual("host5:5000", proxyHosts[0]);
			NUnit.Framework.Assert.AreEqual("host6:6000", proxyHosts[1]);
		}

		internal class MockAmFilterInitializer : AmFilterInitializer
		{
			protected internal override string GetApplicationWebProxyBase()
			{
				return "/foo";
			}

			internal MockAmFilterInitializer(TestAmFilterInitializer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAmFilterInitializer _enclosing;
		}

		internal class MockFilterContainer : FilterContainer
		{
			internal IDictionary<string, string> givenParameters;

			public virtual void AddFilter(string name, string classname, IDictionary<string, 
				string> parameters)
			{
				this.givenParameters = parameters;
			}

			public virtual void AddGlobalFilter(string name, string classname, IDictionary<string
				, string> parameters)
			{
			}

			internal MockFilterContainer(TestAmFilterInitializer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAmFilterInitializer _enclosing;
		}
	}
}
