using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class TestClientRMProxy
	{
		[NUnit.Framework.Test]
		public virtual void TestGetRMDelegationTokenService()
		{
			string defaultRMAddress = YarnConfiguration.DefaultRmAddress;
			YarnConfiguration conf = new YarnConfiguration();
			// HA is not enabled
			Text tokenService = ClientRMProxy.GetRMDelegationTokenService(conf);
			string[] services = tokenService.ToString().Split(",");
			NUnit.Framework.Assert.AreEqual(1, services.Length);
			foreach (string service in services)
			{
				NUnit.Framework.Assert.IsTrue("Incorrect token service name", service.Contains(defaultRMAddress
					));
			}
			// HA is enabled
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.Set(YarnConfiguration.RmHaIds, "rm1,rm2");
			conf.Set(HAUtil.AddSuffix(YarnConfiguration.RmHostname, "rm1"), "0.0.0.0");
			conf.Set(HAUtil.AddSuffix(YarnConfiguration.RmHostname, "rm2"), "0.0.0.0");
			tokenService = ClientRMProxy.GetRMDelegationTokenService(conf);
			services = tokenService.ToString().Split(",");
			NUnit.Framework.Assert.AreEqual(2, services.Length);
			foreach (string service_1 in services)
			{
				NUnit.Framework.Assert.IsTrue("Incorrect token service name", service_1.Contains(
					defaultRMAddress));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestGetAMRMTokenService()
		{
			string defaultRMAddress = YarnConfiguration.DefaultRmSchedulerAddress;
			YarnConfiguration conf = new YarnConfiguration();
			// HA is not enabled
			Text tokenService = ClientRMProxy.GetAMRMTokenService(conf);
			string[] services = tokenService.ToString().Split(",");
			NUnit.Framework.Assert.AreEqual(1, services.Length);
			foreach (string service in services)
			{
				NUnit.Framework.Assert.IsTrue("Incorrect token service name", service.Contains(defaultRMAddress
					));
			}
			// HA is enabled
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.Set(YarnConfiguration.RmHaIds, "rm1,rm2");
			conf.Set(HAUtil.AddSuffix(YarnConfiguration.RmHostname, "rm1"), "0.0.0.0");
			conf.Set(HAUtil.AddSuffix(YarnConfiguration.RmHostname, "rm2"), "0.0.0.0");
			tokenService = ClientRMProxy.GetAMRMTokenService(conf);
			services = tokenService.ToString().Split(",");
			NUnit.Framework.Assert.AreEqual(2, services.Length);
			foreach (string service_1 in services)
			{
				NUnit.Framework.Assert.IsTrue("Incorrect token service name", service_1.Contains(
					defaultRMAddress));
			}
		}
	}
}
