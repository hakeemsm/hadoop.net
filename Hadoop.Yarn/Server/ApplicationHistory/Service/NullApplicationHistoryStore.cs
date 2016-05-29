using System.Collections.Generic;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	/// <summary>
	/// Dummy implementation of
	/// <see cref="ApplicationHistoryStore"/>
	/// . If this
	/// implementation is used, no history data will be persisted.
	/// </summary>
	public class NullApplicationHistoryStore : AbstractService, ApplicationHistoryStore
	{
		public NullApplicationHistoryStore()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.NullApplicationHistoryStore
				).FullName)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationStarted(ApplicationStartData appStart)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationFinished(ApplicationFinishData appFinish)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationAttemptStarted(ApplicationAttemptStartData appAttemptStart
			)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationAttemptFinished(ApplicationAttemptFinishData appAttemptFinish
			)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ContainerStarted(ContainerStartData containerStart)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ContainerFinished(ContainerFinishData containerFinish)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ApplicationHistoryData GetApplication(ApplicationId appId)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ApplicationId, ApplicationHistoryData> GetAllApplications
			()
		{
			return Sharpen.Collections.EmptyMap();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ApplicationAttemptId, ApplicationAttemptHistoryData> GetApplicationAttempts
			(ApplicationId appId)
		{
			return Sharpen.Collections.EmptyMap();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ApplicationAttemptHistoryData GetApplicationAttempt(ApplicationAttemptId
			 appAttemptId)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ContainerHistoryData GetContainer(ContainerId containerId)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ContainerHistoryData GetAMContainer(ApplicationAttemptId appAttemptId
			)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ContainerId, ContainerHistoryData> GetContainers(ApplicationAttemptId
			 appAttemptId)
		{
			return Sharpen.Collections.EmptyMap();
		}
	}
}
