using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public class ApplicationHistoryStoreTestUtils
	{
		protected internal ApplicationHistoryStore store;

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void WriteApplicationStartData(ApplicationId appId)
		{
			store.ApplicationStarted(ApplicationStartData.NewInstance(appId, appId.ToString()
				, "test type", "test queue", "test user", 0, 0));
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void WriteApplicationFinishData(ApplicationId appId)
		{
			store.ApplicationFinished(ApplicationFinishData.NewInstance(appId, 0, appId.ToString
				(), FinalApplicationStatus.Undefined, YarnApplicationState.Finished));
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void WriteApplicationAttemptStartData(ApplicationAttemptId
			 appAttemptId)
		{
			store.ApplicationAttemptStarted(ApplicationAttemptStartData.NewInstance(appAttemptId
				, appAttemptId.ToString(), 0, ContainerId.NewContainerId(appAttemptId, 1)));
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void WriteApplicationAttemptFinishData(ApplicationAttemptId
			 appAttemptId)
		{
			store.ApplicationAttemptFinished(ApplicationAttemptFinishData.NewInstance(appAttemptId
				, appAttemptId.ToString(), "test tracking url", FinalApplicationStatus.Undefined
				, YarnApplicationAttemptState.Finished));
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void WriteContainerStartData(ContainerId containerId)
		{
			store.ContainerStarted(ContainerStartData.NewInstance(containerId, Resource.NewInstance
				(0, 0), NodeId.NewInstance("localhost", 0), Priority.NewInstance(containerId.GetId
				()), 0));
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void WriteContainerFinishData(ContainerId containerId)
		{
			store.ContainerFinished(ContainerFinishData.NewInstance(containerId, 0, containerId
				.ToString(), 0, ContainerState.Complete));
		}
	}
}
