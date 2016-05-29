using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util
{
	public class TestNodeManagerHardwareUtils
	{
		[NUnit.Framework.Test]
		public virtual void TestGetContainerCores()
		{
			YarnConfiguration conf = new YarnConfiguration();
			float ret;
			int numProcessors = 4;
			ResourceCalculatorPlugin plugin = Org.Mockito.Mockito.Mock<ResourceCalculatorPlugin
				>();
			Org.Mockito.Mockito.DoReturn(numProcessors).When(plugin).GetNumProcessors();
			conf.SetInt(YarnConfiguration.NmResourcePercentagePhysicalCpuLimit, 0);
			try
			{
				NodeManagerHardwareUtils.GetContainersCores(plugin, conf);
				NUnit.Framework.Assert.Fail("getContainerCores should have thrown exception");
			}
			catch (ArgumentException)
			{
			}
			// expected
			conf.SetInt(YarnConfiguration.NmResourcePercentagePhysicalCpuLimit, 100);
			ret = NodeManagerHardwareUtils.GetContainersCores(plugin, conf);
			NUnit.Framework.Assert.AreEqual(4, (int)ret);
			conf.SetInt(YarnConfiguration.NmResourcePercentagePhysicalCpuLimit, 50);
			ret = NodeManagerHardwareUtils.GetContainersCores(plugin, conf);
			NUnit.Framework.Assert.AreEqual(2, (int)ret);
			conf.SetInt(YarnConfiguration.NmResourcePercentagePhysicalCpuLimit, 75);
			ret = NodeManagerHardwareUtils.GetContainersCores(plugin, conf);
			NUnit.Framework.Assert.AreEqual(3, (int)ret);
			conf.SetInt(YarnConfiguration.NmResourcePercentagePhysicalCpuLimit, 85);
			ret = NodeManagerHardwareUtils.GetContainersCores(plugin, conf);
			NUnit.Framework.Assert.AreEqual(3.4, ret, 0.1);
			conf.SetInt(YarnConfiguration.NmResourcePercentagePhysicalCpuLimit, 110);
			ret = NodeManagerHardwareUtils.GetContainersCores(plugin, conf);
			NUnit.Framework.Assert.AreEqual(4, (int)ret);
		}
	}
}
