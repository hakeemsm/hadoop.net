using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor
{
	public class TestContainerMetrics
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerMetricsFlow()
		{
			string Err = "Error in number of records";
			// Create a dummy MetricsSystem
			MetricsSystem system = Org.Mockito.Mockito.Mock<MetricsSystem>();
			Org.Mockito.Mockito.DoReturn(this).When(system).Register(Matchers.AnyString(), Matchers.AnyString
				(), Matchers.Any());
			MetricsCollectorImpl collector = new MetricsCollectorImpl();
			ContainerId containerId = Org.Mockito.Mockito.Mock<ContainerId>();
			ContainerMetrics metrics = ContainerMetrics.ForContainer(containerId, 100, 1);
			metrics.RecordMemoryUsage(1024);
			metrics.GetMetrics(collector, true);
			NUnit.Framework.Assert.AreEqual(Err, 0, collector.GetRecords().Count);
			Sharpen.Thread.Sleep(110);
			metrics.GetMetrics(collector, true);
			NUnit.Framework.Assert.AreEqual(Err, 1, collector.GetRecords().Count);
			collector.Clear();
			Sharpen.Thread.Sleep(110);
			metrics.GetMetrics(collector, true);
			NUnit.Framework.Assert.AreEqual(Err, 1, collector.GetRecords().Count);
			collector.Clear();
			metrics.Finished();
			metrics.GetMetrics(collector, true);
			NUnit.Framework.Assert.AreEqual(Err, 1, collector.GetRecords().Count);
			collector.Clear();
			metrics.GetMetrics(collector, true);
			NUnit.Framework.Assert.AreEqual(Err, 0, collector.GetRecords().Count);
			Sharpen.Thread.Sleep(110);
			metrics.GetMetrics(collector, true);
			NUnit.Framework.Assert.AreEqual(Err, 0, collector.GetRecords().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerMetricsLimit()
		{
			string Err = "Error in number of records";
			MetricsSystem system = Org.Mockito.Mockito.Mock<MetricsSystem>();
			Org.Mockito.Mockito.DoReturn(this).When(system).Register(Matchers.AnyString(), Matchers.AnyString
				(), Matchers.Any());
			MetricsCollectorImpl collector = new MetricsCollectorImpl();
			ContainerId containerId = Org.Mockito.Mockito.Mock<ContainerId>();
			ContainerMetrics metrics = ContainerMetrics.ForContainer(containerId, 100, 1);
			int anyPmemLimit = 1024;
			int anyVmemLimit = 2048;
			int anyVcores = 10;
			string anyProcessId = "1234";
			metrics.RecordResourceLimit(anyVmemLimit, anyPmemLimit, anyVcores);
			metrics.RecordProcessId(anyProcessId);
			Sharpen.Thread.Sleep(110);
			metrics.GetMetrics(collector, true);
			NUnit.Framework.Assert.AreEqual(Err, 1, collector.GetRecords().Count);
			MetricsRecord record = collector.GetRecords()[0];
			MetricsRecords.AssertTag(record, ContainerMetrics.ProcessidInfo.Name(), anyProcessId
				);
			MetricsRecords.AssertMetric(record, ContainerMetrics.PmemLimitMetricName, anyPmemLimit
				);
			MetricsRecords.AssertMetric(record, ContainerMetrics.VmemLimitMetricName, anyVmemLimit
				);
			MetricsRecords.AssertMetric(record, ContainerMetrics.VcoreLimitMetricName, anyVcores
				);
			collector.Clear();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerMetricsFinished()
		{
			MetricsSystemImpl system = new MetricsSystemImpl();
			system.Init("test");
			MetricsCollectorImpl collector = new MetricsCollectorImpl();
			ApplicationId appId = ApplicationId.NewInstance(1234, 3);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 4);
			ContainerId containerId1 = ContainerId.NewContainerId(appAttemptId, 1);
			ContainerMetrics metrics1 = ContainerMetrics.ForContainer(system, containerId1, 1
				, 0);
			ContainerId containerId2 = ContainerId.NewContainerId(appAttemptId, 2);
			ContainerMetrics metrics2 = ContainerMetrics.ForContainer(system, containerId2, 1
				, 0);
			ContainerId containerId3 = ContainerId.NewContainerId(appAttemptId, 3);
			ContainerMetrics metrics3 = ContainerMetrics.ForContainer(system, containerId3, 1
				, 0);
			metrics1.Finished();
			metrics2.Finished();
			system.SampleMetrics();
			system.SampleMetrics();
			Sharpen.Thread.Sleep(100);
			system.Stop();
			// verify metrics1 is unregistered
			NUnit.Framework.Assert.IsTrue(metrics1 != ContainerMetrics.ForContainer(system, containerId1
				, 1, 0));
			// verify metrics2 is unregistered
			NUnit.Framework.Assert.IsTrue(metrics2 != ContainerMetrics.ForContainer(system, containerId2
				, 1, 0));
			// verify metrics3 is still registered
			NUnit.Framework.Assert.IsTrue(metrics3 == ContainerMetrics.ForContainer(system, containerId3
				, 1, 0));
			system.Shutdown();
		}
	}
}
