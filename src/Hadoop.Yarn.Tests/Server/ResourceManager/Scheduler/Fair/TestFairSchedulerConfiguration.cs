using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestFairSchedulerConfiguration
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestParseResourceConfigValue()
		{
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewResource(1024, 2), FairSchedulerConfiguration.ParseResourceConfigValue
				("2 vcores, 1024 mb"));
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewResource(1024, 2), FairSchedulerConfiguration.ParseResourceConfigValue
				("1024 mb, 2 vcores"));
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewResource(1024, 2), FairSchedulerConfiguration.ParseResourceConfigValue
				("2vcores,1024mb"));
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewResource(1024, 2), FairSchedulerConfiguration.ParseResourceConfigValue
				("1024mb,2vcores"));
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewResource(1024, 2), FairSchedulerConfiguration.ParseResourceConfigValue
				("1024   mb, 2    vcores"));
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewResource(1024, 2), FairSchedulerConfiguration.ParseResourceConfigValue
				("1024 Mb, 2 vCores"));
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewResource(1024, 2), FairSchedulerConfiguration.ParseResourceConfigValue
				("  1024 mb, 2 vcores  "));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNoUnits()
		{
			FairSchedulerConfiguration.ParseResourceConfigValue("1024");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestOnlyMemory()
		{
			FairSchedulerConfiguration.ParseResourceConfigValue("1024mb");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestOnlyCPU()
		{
			FairSchedulerConfiguration.ParseResourceConfigValue("1024vcores");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGibberish()
		{
			FairSchedulerConfiguration.ParseResourceConfigValue("1o24vc0res");
		}
	}
}
