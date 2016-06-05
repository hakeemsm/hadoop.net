using System;
using System.Net;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestMaster
	{
		[NUnit.Framework.Test]
		public virtual void TestGetMasterAddress()
		{
			YarnConfiguration conf = new YarnConfiguration();
			// Default is yarn framework
			string masterHostname = Master.GetMasterAddress(conf).GetHostName();
			// no address set so should default to default rm address
			IPEndPoint rmAddr = NetUtils.CreateSocketAddr(YarnConfiguration.DefaultRmAddress);
			NUnit.Framework.Assert.AreEqual(masterHostname, rmAddr.GetHostName());
			// Trying invalid master address for classic 
			conf.Set(MRConfig.FrameworkName, MRConfig.ClassicFrameworkName);
			conf.Set(MRConfig.MasterAddress, "local:invalid");
			// should throw an exception for invalid value
			try
			{
				Master.GetMasterAddress(conf);
				NUnit.Framework.Assert.Fail("Should not reach here as there is a bad master address"
					);
			}
			catch (Exception)
			{
			}
			// Expected
			// Change master address to a valid value
			conf.Set(MRConfig.MasterAddress, "bar.com:8042");
			masterHostname = Master.GetMasterAddress(conf).GetHostName();
			NUnit.Framework.Assert.AreEqual(masterHostname, "bar.com");
			// change framework to yarn
			conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			conf.Set(YarnConfiguration.RmAddress, "foo1.com:8192");
			masterHostname = Master.GetMasterAddress(conf).GetHostName();
			NUnit.Framework.Assert.AreEqual(masterHostname, "foo1.com");
		}

		[NUnit.Framework.Test]
		public virtual void TestGetMasterUser()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(MRConfig.MasterUserName, "foo");
			conf.Set(YarnConfiguration.RmPrincipal, "bar");
			// default is yarn framework  
			NUnit.Framework.Assert.AreEqual(Master.GetMasterUserName(conf), "bar");
			// set framework name to classic
			conf.Set(MRConfig.FrameworkName, MRConfig.ClassicFrameworkName);
			NUnit.Framework.Assert.AreEqual(Master.GetMasterUserName(conf), "foo");
			// change framework to yarn
			conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			NUnit.Framework.Assert.AreEqual(Master.GetMasterUserName(conf), "bar");
		}
	}
}
