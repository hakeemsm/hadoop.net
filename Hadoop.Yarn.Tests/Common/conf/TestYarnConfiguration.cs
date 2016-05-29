using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Conf
{
	public class TestYarnConfiguration
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultRMWebUrl()
		{
			YarnConfiguration conf = new YarnConfiguration();
			string rmWebUrl = WebAppUtils.GetRMWebAppURLWithScheme(conf);
			// shouldn't have a "/" on the end of the url as all the other uri routinnes
			// specifically add slashes and Jetty doesn't handle double slashes.
			NUnit.Framework.Assert.AreNotSame("RM Web Url is not correct", "http://0.0.0.0:8088"
				, rmWebUrl);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMWebUrlSpecified()
		{
			YarnConfiguration conf = new YarnConfiguration();
			// seems a bit odd but right now we are forcing webapp for RM to be
			// RM_ADDRESS
			// for host and use the port from the RM_WEBAPP_ADDRESS
			conf.Set(YarnConfiguration.RmWebappAddress, "fortesting:24543");
			conf.Set(YarnConfiguration.RmAddress, "rmtesting:9999");
			string rmWebUrl = WebAppUtils.GetRMWebAppURLWithScheme(conf);
			string[] parts = rmWebUrl.Split(":");
			NUnit.Framework.Assert.AreEqual("RM Web URL Port is incrrect", 24543, Sharpen.Extensions.ValueOf
				(parts[parts.Length - 1]));
			NUnit.Framework.Assert.AreNotSame("RM Web Url not resolved correctly. Should not be rmtesting"
				, "http://rmtesting:24543", rmWebUrl);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetSocketAddressForNMWithHA()
		{
			YarnConfiguration conf = new YarnConfiguration();
			// Set NM address
			conf.Set(YarnConfiguration.NmAddress, "0.0.0.0:1234");
			// Set HA
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.Set(YarnConfiguration.RmHaId, "rm1");
			NUnit.Framework.Assert.IsTrue(HAUtil.IsHAEnabled(conf));
			IPEndPoint addr = conf.GetSocketAddr(YarnConfiguration.NmAddress, YarnConfiguration
				.DefaultNmAddress, YarnConfiguration.DefaultNmPort);
			NUnit.Framework.Assert.AreEqual(1234, addr.Port);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetSocketAddr()
		{
			YarnConfiguration conf;
			IPEndPoint resourceTrackerAddress;
			//all default
			conf = new YarnConfiguration();
			resourceTrackerAddress = conf.GetSocketAddr(YarnConfiguration.RmBindHost, YarnConfiguration
				.RmResourceTrackerAddress, YarnConfiguration.DefaultRmResourceTrackerAddress, YarnConfiguration
				.DefaultRmResourceTrackerPort);
			NUnit.Framework.Assert.AreEqual(new IPEndPoint(YarnConfiguration.DefaultRmResourceTrackerAddress
				.Split(":")[0], YarnConfiguration.DefaultRmResourceTrackerPort), resourceTrackerAddress
				);
			//with address
			conf.Set(YarnConfiguration.RmResourceTrackerAddress, "10.0.0.1");
			resourceTrackerAddress = conf.GetSocketAddr(YarnConfiguration.RmBindHost, YarnConfiguration
				.RmResourceTrackerAddress, YarnConfiguration.DefaultRmResourceTrackerAddress, YarnConfiguration
				.DefaultRmResourceTrackerPort);
			NUnit.Framework.Assert.AreEqual(new IPEndPoint("10.0.0.1", YarnConfiguration.DefaultRmResourceTrackerPort
				), resourceTrackerAddress);
			//address and socket
			conf.Set(YarnConfiguration.RmResourceTrackerAddress, "10.0.0.2:5001");
			resourceTrackerAddress = conf.GetSocketAddr(YarnConfiguration.RmBindHost, YarnConfiguration
				.RmResourceTrackerAddress, YarnConfiguration.DefaultRmResourceTrackerAddress, YarnConfiguration
				.DefaultRmResourceTrackerPort);
			NUnit.Framework.Assert.AreEqual(new IPEndPoint("10.0.0.2", 5001), resourceTrackerAddress
				);
			//bind host only
			conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.RmBindHost, "10.0.0.3");
			resourceTrackerAddress = conf.GetSocketAddr(YarnConfiguration.RmBindHost, YarnConfiguration
				.RmResourceTrackerAddress, YarnConfiguration.DefaultRmResourceTrackerAddress, YarnConfiguration
				.DefaultRmResourceTrackerPort);
			NUnit.Framework.Assert.AreEqual(new IPEndPoint("10.0.0.3", YarnConfiguration.DefaultRmResourceTrackerPort
				), resourceTrackerAddress);
			//bind host and address no port
			conf.Set(YarnConfiguration.RmBindHost, "0.0.0.0");
			conf.Set(YarnConfiguration.RmResourceTrackerAddress, "10.0.0.2");
			resourceTrackerAddress = conf.GetSocketAddr(YarnConfiguration.RmBindHost, YarnConfiguration
				.RmResourceTrackerAddress, YarnConfiguration.DefaultRmResourceTrackerAddress, YarnConfiguration
				.DefaultRmResourceTrackerPort);
			NUnit.Framework.Assert.AreEqual(new IPEndPoint("0.0.0.0", YarnConfiguration.DefaultRmResourceTrackerPort
				), resourceTrackerAddress);
			//bind host and address with port
			conf.Set(YarnConfiguration.RmBindHost, "0.0.0.0");
			conf.Set(YarnConfiguration.RmResourceTrackerAddress, "10.0.0.2:5003");
			resourceTrackerAddress = conf.GetSocketAddr(YarnConfiguration.RmBindHost, YarnConfiguration
				.RmResourceTrackerAddress, YarnConfiguration.DefaultRmResourceTrackerAddress, YarnConfiguration
				.DefaultRmResourceTrackerPort);
			NUnit.Framework.Assert.AreEqual(new IPEndPoint("0.0.0.0", 5003), resourceTrackerAddress
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdateConnectAddr()
		{
			YarnConfiguration conf;
			IPEndPoint resourceTrackerConnectAddress;
			IPEndPoint serverAddress;
			//no override, old behavior.  Won't work on a host named "yo.yo.yo"
			conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.RmResourceTrackerAddress, "yo.yo.yo");
			serverAddress = new IPEndPoint(YarnConfiguration.DefaultRmResourceTrackerAddress.
				Split(":")[0], Sharpen.Extensions.ValueOf(YarnConfiguration.DefaultRmResourceTrackerAddress
				.Split(":")[1]));
			resourceTrackerConnectAddress = conf.UpdateConnectAddr(YarnConfiguration.RmBindHost
				, YarnConfiguration.RmResourceTrackerAddress, YarnConfiguration.DefaultRmResourceTrackerAddress
				, serverAddress);
			NUnit.Framework.Assert.IsFalse(resourceTrackerConnectAddress.ToString().StartsWith
				("yo.yo.yo"));
			//cause override with address
			conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.RmResourceTrackerAddress, "yo.yo.yo");
			conf.Set(YarnConfiguration.RmBindHost, "0.0.0.0");
			serverAddress = new IPEndPoint(YarnConfiguration.DefaultRmResourceTrackerAddress.
				Split(":")[0], Sharpen.Extensions.ValueOf(YarnConfiguration.DefaultRmResourceTrackerAddress
				.Split(":")[1]));
			resourceTrackerConnectAddress = conf.UpdateConnectAddr(YarnConfiguration.RmBindHost
				, YarnConfiguration.RmResourceTrackerAddress, YarnConfiguration.DefaultRmResourceTrackerAddress
				, serverAddress);
			NUnit.Framework.Assert.IsTrue(resourceTrackerConnectAddress.ToString().StartsWith
				("yo.yo.yo"));
		}
	}
}
