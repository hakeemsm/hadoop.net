using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	/// <summary>
	/// In-memory implementation of
	/// <see cref="ApplicationHistoryStore"/>
	/// . This
	/// implementation is for test purpose only. If users improperly instantiate it,
	/// they may encounter reading and writing history data in different memory
	/// store.
	/// </summary>
	public class MemoryApplicationHistoryStore : AbstractService, ApplicationHistoryStore
	{
		private readonly ConcurrentMap<ApplicationId, ApplicationHistoryData> applicationData
			 = new ConcurrentHashMap<ApplicationId, ApplicationHistoryData>();

		private readonly ConcurrentMap<ApplicationId, ConcurrentMap<ApplicationAttemptId, 
			ApplicationAttemptHistoryData>> applicationAttemptData = new ConcurrentHashMap<ApplicationId
			, ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData>>();

		private readonly ConcurrentMap<ApplicationAttemptId, ConcurrentMap<ContainerId, ContainerHistoryData
			>> containerData = new ConcurrentHashMap<ApplicationAttemptId, ConcurrentMap<ContainerId
			, ContainerHistoryData>>();

		public MemoryApplicationHistoryStore()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.MemoryApplicationHistoryStore
				).FullName)
		{
		}

		public virtual IDictionary<ApplicationId, ApplicationHistoryData> GetAllApplications
			()
		{
			return new Dictionary<ApplicationId, ApplicationHistoryData>(applicationData);
		}

		public virtual ApplicationHistoryData GetApplication(ApplicationId appId)
		{
			return applicationData[appId];
		}

		public virtual IDictionary<ApplicationAttemptId, ApplicationAttemptHistoryData> GetApplicationAttempts
			(ApplicationId appId)
		{
			ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData> subMap = applicationAttemptData
				[appId];
			if (subMap == null)
			{
				return Sharpen.Collections.EmptyMap<ApplicationAttemptId, ApplicationAttemptHistoryData
					>();
			}
			else
			{
				return new Dictionary<ApplicationAttemptId, ApplicationAttemptHistoryData>(subMap
					);
			}
		}

		public virtual ApplicationAttemptHistoryData GetApplicationAttempt(ApplicationAttemptId
			 appAttemptId)
		{
			ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData> subMap = applicationAttemptData
				[appAttemptId.GetApplicationId()];
			if (subMap == null)
			{
				return null;
			}
			else
			{
				return subMap[appAttemptId];
			}
		}

		public virtual ContainerHistoryData GetAMContainer(ApplicationAttemptId appAttemptId
			)
		{
			ApplicationAttemptHistoryData appAttempt = GetApplicationAttempt(appAttemptId);
			if (appAttempt == null || appAttempt.GetMasterContainerId() == null)
			{
				return null;
			}
			else
			{
				return GetContainer(appAttempt.GetMasterContainerId());
			}
		}

		public virtual ContainerHistoryData GetContainer(ContainerId containerId)
		{
			IDictionary<ContainerId, ContainerHistoryData> subMap = containerData[containerId
				.GetApplicationAttemptId()];
			if (subMap == null)
			{
				return null;
			}
			else
			{
				return subMap[containerId];
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ContainerId, ContainerHistoryData> GetContainers(ApplicationAttemptId
			 appAttemptId)
		{
			ConcurrentMap<ContainerId, ContainerHistoryData> subMap = containerData[appAttemptId
				];
			if (subMap == null)
			{
				return Sharpen.Collections.EmptyMap<ContainerId, ContainerHistoryData>();
			}
			else
			{
				return new Dictionary<ContainerId, ContainerHistoryData>(subMap);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationStarted(ApplicationStartData appStart)
		{
			ApplicationHistoryData oldData = applicationData.PutIfAbsent(appStart.GetApplicationId
				(), ApplicationHistoryData.NewInstance(appStart.GetApplicationId(), appStart.GetApplicationName
				(), appStart.GetApplicationType(), appStart.GetQueue(), appStart.GetUser(), appStart
				.GetSubmitTime(), appStart.GetStartTime(), long.MaxValue, null, null, null));
			if (oldData != null)
			{
				throw new IOException("The start information of application " + appStart.GetApplicationId
					() + " is already stored.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationFinished(ApplicationFinishData appFinish)
		{
			ApplicationHistoryData data = applicationData[appFinish.GetApplicationId()];
			if (data == null)
			{
				throw new IOException("The finish information of application " + appFinish.GetApplicationId
					() + " is stored before the start" + " information.");
			}
			// Make the assumption that YarnApplicationState should not be null if
			// the finish information is already recorded
			if (data.GetYarnApplicationState() != null)
			{
				throw new IOException("The finish information of application " + appFinish.GetApplicationId
					() + " is already stored.");
			}
			data.SetFinishTime(appFinish.GetFinishTime());
			data.SetDiagnosticsInfo(appFinish.GetDiagnosticsInfo());
			data.SetFinalApplicationStatus(appFinish.GetFinalApplicationStatus());
			data.SetYarnApplicationState(appFinish.GetYarnApplicationState());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationAttemptStarted(ApplicationAttemptStartData appAttemptStart
			)
		{
			ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData> subMap = GetSubMap
				(appAttemptStart.GetApplicationAttemptId().GetApplicationId());
			ApplicationAttemptHistoryData oldData = subMap.PutIfAbsent(appAttemptStart.GetApplicationAttemptId
				(), ApplicationAttemptHistoryData.NewInstance(appAttemptStart.GetApplicationAttemptId
				(), appAttemptStart.GetHost(), appAttemptStart.GetRPCPort(), appAttemptStart.GetMasterContainerId
				(), null, null, null, null));
			if (oldData != null)
			{
				throw new IOException("The start information of application attempt " + appAttemptStart
					.GetApplicationAttemptId() + " is already stored.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationAttemptFinished(ApplicationAttemptFinishData appAttemptFinish
			)
		{
			ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData> subMap = GetSubMap
				(appAttemptFinish.GetApplicationAttemptId().GetApplicationId());
			ApplicationAttemptHistoryData data = subMap[appAttemptFinish.GetApplicationAttemptId
				()];
			if (data == null)
			{
				throw new IOException("The finish information of application attempt " + appAttemptFinish
					.GetApplicationAttemptId() + " is stored before" + " the start information.");
			}
			// Make the assumption that YarnApplicationAttemptState should not be null
			// if the finish information is already recorded
			if (data.GetYarnApplicationAttemptState() != null)
			{
				throw new IOException("The finish information of application attempt " + appAttemptFinish
					.GetApplicationAttemptId() + " is already stored.");
			}
			data.SetTrackingURL(appAttemptFinish.GetTrackingURL());
			data.SetDiagnosticsInfo(appAttemptFinish.GetDiagnosticsInfo());
			data.SetFinalApplicationStatus(appAttemptFinish.GetFinalApplicationStatus());
			data.SetYarnApplicationAttemptState(appAttemptFinish.GetYarnApplicationAttemptState
				());
		}

		private ConcurrentMap<ApplicationAttemptId, ApplicationAttemptHistoryData> GetSubMap
			(ApplicationId appId)
		{
			applicationAttemptData.PutIfAbsent(appId, new ConcurrentHashMap<ApplicationAttemptId
				, ApplicationAttemptHistoryData>());
			return applicationAttemptData[appId];
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ContainerStarted(ContainerStartData containerStart)
		{
			ConcurrentMap<ContainerId, ContainerHistoryData> subMap = GetSubMap(containerStart
				.GetContainerId().GetApplicationAttemptId());
			ContainerHistoryData oldData = subMap.PutIfAbsent(containerStart.GetContainerId()
				, ContainerHistoryData.NewInstance(containerStart.GetContainerId(), containerStart
				.GetAllocatedResource(), containerStart.GetAssignedNode(), containerStart.GetPriority
				(), containerStart.GetStartTime(), long.MaxValue, null, int.MaxValue, null));
			if (oldData != null)
			{
				throw new IOException("The start information of container " + containerStart.GetContainerId
					() + " is already stored.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ContainerFinished(ContainerFinishData containerFinish)
		{
			ConcurrentMap<ContainerId, ContainerHistoryData> subMap = GetSubMap(containerFinish
				.GetContainerId().GetApplicationAttemptId());
			ContainerHistoryData data = subMap[containerFinish.GetContainerId()];
			if (data == null)
			{
				throw new IOException("The finish information of container " + containerFinish.GetContainerId
					() + " is stored before" + " the start information.");
			}
			// Make the assumption that ContainerState should not be null if
			// the finish information is already recorded
			if (data.GetContainerState() != null)
			{
				throw new IOException("The finish information of container " + containerFinish.GetContainerId
					() + " is already stored.");
			}
			data.SetFinishTime(containerFinish.GetFinishTime());
			data.SetDiagnosticsInfo(containerFinish.GetDiagnosticsInfo());
			data.SetContainerExitStatus(containerFinish.GetContainerExitStatus());
			data.SetContainerState(containerFinish.GetContainerState());
		}

		private ConcurrentMap<ContainerId, ContainerHistoryData> GetSubMap(ApplicationAttemptId
			 appAttemptId)
		{
			containerData.PutIfAbsent(appAttemptId, new ConcurrentHashMap<ContainerId, ContainerHistoryData
				>());
			return containerData[appAttemptId];
		}
	}
}
