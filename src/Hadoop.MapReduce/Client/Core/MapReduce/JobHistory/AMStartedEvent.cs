using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record start of a task attempt</summary>
	public class AMStartedEvent : HistoryEvent
	{
		private AMStarted datum = new AMStarted();

		private string forcedJobStateOnShutDown;

		private long submitTime;

		/// <summary>Create an event to record the start of an MR AppMaster</summary>
		/// <param name="appAttemptId">the application attempt id.</param>
		/// <param name="startTime">the start time of the AM.</param>
		/// <param name="containerId">the containerId of the AM.</param>
		/// <param name="nodeManagerHost">the node on which the AM is running.</param>
		/// <param name="nodeManagerPort">the port on which the AM is running.</param>
		/// <param name="nodeManagerHttpPort">the httpPort for the node running the AM.</param>
		public AMStartedEvent(ApplicationAttemptId appAttemptId, long startTime, ContainerId
			 containerId, string nodeManagerHost, int nodeManagerPort, int nodeManagerHttpPort
			, long submitTime)
			: this(appAttemptId, startTime, containerId, nodeManagerHost, nodeManagerPort, nodeManagerHttpPort
				, null, submitTime)
		{
		}

		/// <summary>Create an event to record the start of an MR AppMaster</summary>
		/// <param name="appAttemptId">the application attempt id.</param>
		/// <param name="startTime">the start time of the AM.</param>
		/// <param name="containerId">the containerId of the AM.</param>
		/// <param name="nodeManagerHost">the node on which the AM is running.</param>
		/// <param name="nodeManagerPort">the port on which the AM is running.</param>
		/// <param name="nodeManagerHttpPort">the httpPort for the node running the AM.</param>
		/// <param name="forcedJobStateOnShutDown">the state to force the job into</param>
		public AMStartedEvent(ApplicationAttemptId appAttemptId, long startTime, ContainerId
			 containerId, string nodeManagerHost, int nodeManagerPort, int nodeManagerHttpPort
			, string forcedJobStateOnShutDown, long submitTime)
		{
			datum.applicationAttemptId = new Utf8(appAttemptId.ToString());
			datum.startTime = startTime;
			datum.containerId = new Utf8(containerId.ToString());
			datum.nodeManagerHost = new Utf8(nodeManagerHost);
			datum.nodeManagerPort = nodeManagerPort;
			datum.nodeManagerHttpPort = nodeManagerHttpPort;
			this.forcedJobStateOnShutDown = forcedJobStateOnShutDown;
			this.submitTime = submitTime;
		}

		internal AMStartedEvent()
		{
		}

		public virtual object GetDatum()
		{
			return datum;
		}

		public virtual void SetDatum(object datum)
		{
			this.datum = (AMStarted)datum;
		}

		/// <returns>the ApplicationAttemptId</returns>
		public virtual ApplicationAttemptId GetAppAttemptId()
		{
			return ConverterUtils.ToApplicationAttemptId(datum.applicationAttemptId.ToString(
				));
		}

		/// <returns>the start time for the MRAppMaster</returns>
		public virtual long GetStartTime()
		{
			return datum.startTime;
		}

		/// <returns>the ContainerId for the MRAppMaster.</returns>
		public virtual ContainerId GetContainerId()
		{
			return ConverterUtils.ToContainerId(datum.containerId.ToString());
		}

		/// <returns>the node manager host.</returns>
		public virtual string GetNodeManagerHost()
		{
			return datum.nodeManagerHost.ToString();
		}

		/// <returns>the node manager port.</returns>
		public virtual int GetNodeManagerPort()
		{
			return datum.nodeManagerPort;
		}

		/// <returns>the http port for the tracker.</returns>
		public virtual int GetNodeManagerHttpPort()
		{
			return datum.nodeManagerHttpPort;
		}

		/// <returns>the state to force the job into</returns>
		public virtual string GetForcedJobStateOnShutDown()
		{
			return this.forcedJobStateOnShutDown;
		}

		/// <returns>the submit time for the Application(Job)</returns>
		public virtual long GetSubmitTime()
		{
			return this.submitTime;
		}

		/// <summary>Get the attempt id</summary>
		public virtual EventType GetEventType()
		{
			return EventType.AmStarted;
		}
	}
}
