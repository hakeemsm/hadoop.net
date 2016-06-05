using NUnit.Framework;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestClusterMetrics
	{
		private ClusterMetrics metrics;

		/// <summary>Test aMLaunchDelay and aMRegisterDelay Metrics</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAmMetrics()
		{
			System.Diagnostics.Debug.Assert((metrics != null));
			NUnit.Framework.Assert.IsTrue(!metrics.aMLaunchDelay.Changed());
			NUnit.Framework.Assert.IsTrue(!metrics.aMRegisterDelay.Changed());
			metrics.AddAMLaunchDelay(1);
			metrics.AddAMRegisterDelay(1);
			NUnit.Framework.Assert.IsTrue(metrics.aMLaunchDelay.Changed());
			NUnit.Framework.Assert.IsTrue(metrics.aMRegisterDelay.Changed());
		}

		[SetUp]
		public virtual void Setup()
		{
			DefaultMetricsSystem.Initialize("ResourceManager");
			metrics = ClusterMetrics.GetMetrics();
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			ClusterMetrics.Destroy();
			MetricsSystem ms = DefaultMetricsSystem.Instance();
			if (ms.GetSource("ClusterMetrics") != null)
			{
				DefaultMetricsSystem.Shutdown();
			}
		}
	}
}
