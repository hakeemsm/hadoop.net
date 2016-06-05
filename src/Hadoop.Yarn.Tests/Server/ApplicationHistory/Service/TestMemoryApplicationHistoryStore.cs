using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public class TestMemoryApplicationHistoryStore : ApplicationHistoryStoreTestUtils
	{
		[SetUp]
		public virtual void Setup()
		{
			store = new MemoryApplicationHistoryStore();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReadWriteApplicationHistory()
		{
			// Out of order
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			try
			{
				WriteApplicationFinishData(appId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is stored before the start information"
					));
			}
			// Normal
			int numApps = 5;
			for (int i = 1; i <= numApps; ++i)
			{
				appId = ApplicationId.NewInstance(0, i);
				WriteApplicationStartData(appId);
				WriteApplicationFinishData(appId);
			}
			NUnit.Framework.Assert.AreEqual(numApps, store.GetAllApplications().Count);
			for (int i_1 = 1; i_1 <= numApps; ++i_1)
			{
				appId = ApplicationId.NewInstance(0, i_1);
				ApplicationHistoryData data = store.GetApplication(appId);
				NUnit.Framework.Assert.IsNotNull(data);
				NUnit.Framework.Assert.AreEqual(appId.ToString(), data.GetApplicationName());
				NUnit.Framework.Assert.AreEqual(appId.ToString(), data.GetDiagnosticsInfo());
			}
			// Write again
			appId = ApplicationId.NewInstance(0, 1);
			try
			{
				WriteApplicationStartData(appId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is already stored"));
			}
			try
			{
				WriteApplicationFinishData(appId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is already stored"));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReadWriteApplicationAttemptHistory()
		{
			// Out of order
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			try
			{
				WriteApplicationAttemptFinishData(appAttemptId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is stored before the start information"
					));
			}
			// Normal
			int numAppAttempts = 5;
			WriteApplicationStartData(appId);
			for (int i = 1; i <= numAppAttempts; ++i)
			{
				appAttemptId = ApplicationAttemptId.NewInstance(appId, i);
				WriteApplicationAttemptStartData(appAttemptId);
				WriteApplicationAttemptFinishData(appAttemptId);
			}
			NUnit.Framework.Assert.AreEqual(numAppAttempts, store.GetApplicationAttempts(appId
				).Count);
			for (int i_1 = 1; i_1 <= numAppAttempts; ++i_1)
			{
				appAttemptId = ApplicationAttemptId.NewInstance(appId, i_1);
				ApplicationAttemptHistoryData data = store.GetApplicationAttempt(appAttemptId);
				NUnit.Framework.Assert.IsNotNull(data);
				NUnit.Framework.Assert.AreEqual(appAttemptId.ToString(), data.GetHost());
				NUnit.Framework.Assert.AreEqual(appAttemptId.ToString(), data.GetDiagnosticsInfo(
					));
			}
			WriteApplicationFinishData(appId);
			// Write again
			appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			try
			{
				WriteApplicationAttemptStartData(appAttemptId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is already stored"));
			}
			try
			{
				WriteApplicationAttemptFinishData(appAttemptId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is already stored"));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReadWriteContainerHistory()
		{
			// Out of order
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 1);
			try
			{
				WriteContainerFinishData(containerId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is stored before the start information"
					));
			}
			// Normal
			WriteApplicationAttemptStartData(appAttemptId);
			int numContainers = 5;
			for (int i = 1; i <= numContainers; ++i)
			{
				containerId = ContainerId.NewContainerId(appAttemptId, i);
				WriteContainerStartData(containerId);
				WriteContainerFinishData(containerId);
			}
			NUnit.Framework.Assert.AreEqual(numContainers, store.GetContainers(appAttemptId).
				Count);
			for (int i_1 = 1; i_1 <= numContainers; ++i_1)
			{
				containerId = ContainerId.NewContainerId(appAttemptId, i_1);
				ContainerHistoryData data = store.GetContainer(containerId);
				NUnit.Framework.Assert.IsNotNull(data);
				NUnit.Framework.Assert.AreEqual(Priority.NewInstance(containerId.GetId()), data.GetPriority
					());
				NUnit.Framework.Assert.AreEqual(containerId.ToString(), data.GetDiagnosticsInfo()
					);
			}
			ContainerHistoryData masterContainer = store.GetAMContainer(appAttemptId);
			NUnit.Framework.Assert.IsNotNull(masterContainer);
			NUnit.Framework.Assert.AreEqual(ContainerId.NewContainerId(appAttemptId, 1), masterContainer
				.GetContainerId());
			WriteApplicationAttemptFinishData(appAttemptId);
			// Write again
			containerId = ContainerId.NewContainerId(appAttemptId, 1);
			try
			{
				WriteContainerStartData(containerId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is already stored"));
			}
			try
			{
				WriteContainerFinishData(containerId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is already stored"));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMassiveWriteContainerHistory()
		{
			long mb = 1024 * 1024;
			Runtime runtime = Runtime.GetRuntime();
			long usedMemoryBefore = (runtime.TotalMemory() - runtime.FreeMemory()) / mb;
			int numContainers = 100000;
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			for (int i = 1; i <= numContainers; ++i)
			{
				ContainerId containerId = ContainerId.NewContainerId(appAttemptId, i);
				WriteContainerStartData(containerId);
				WriteContainerFinishData(containerId);
			}
			long usedMemoryAfter = (runtime.TotalMemory() - runtime.FreeMemory()) / mb;
			NUnit.Framework.Assert.IsTrue((usedMemoryAfter - usedMemoryBefore) < 400);
		}
	}
}
