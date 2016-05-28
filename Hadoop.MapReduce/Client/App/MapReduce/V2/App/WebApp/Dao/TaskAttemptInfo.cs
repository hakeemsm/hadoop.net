using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class TaskAttemptInfo
	{
		protected internal long startTime;

		protected internal long finishTime;

		protected internal long elapsedTime;

		protected internal float progress;

		protected internal string id;

		protected internal string rack;

		protected internal TaskAttemptState state;

		protected internal string status;

		protected internal string nodeHttpAddress;

		protected internal string diagnostics;

		protected internal string type;

		protected internal string assignedContainerId;

		[XmlTransient]
		protected internal ContainerId assignedContainer;

		public TaskAttemptInfo()
		{
		}

		public TaskAttemptInfo(TaskAttempt ta, bool isRunning)
			: this(ta, TaskType.Map, isRunning)
		{
		}

		public TaskAttemptInfo(TaskAttempt ta, TaskType type, bool isRunning)
		{
			TaskAttemptReport report = ta.GetReport();
			this.type = type.ToString();
			this.id = MRApps.ToString(ta.GetID());
			this.nodeHttpAddress = ta.GetNodeHttpAddress();
			this.startTime = report.GetStartTime();
			this.finishTime = report.GetFinishTime();
			this.assignedContainerId = ConverterUtils.ToString(report.GetContainerId());
			this.assignedContainer = report.GetContainerId();
			this.progress = report.GetProgress() * 100;
			this.status = report.GetStateString();
			this.state = report.GetTaskAttemptState();
			this.elapsedTime = Times.Elapsed(this.startTime, this.finishTime, isRunning);
			if (this.elapsedTime == -1)
			{
				this.elapsedTime = 0;
			}
			this.diagnostics = report.GetDiagnosticInfo();
			this.rack = ta.GetNodeRackName();
		}

		public virtual string GetAssignedContainerIdStr()
		{
			return this.assignedContainerId;
		}

		public virtual ContainerId GetAssignedContainerId()
		{
			return this.assignedContainer;
		}

		public virtual string GetState()
		{
			return this.state.ToString();
		}

		public virtual string GetStatus()
		{
			return status;
		}

		public virtual string GetId()
		{
			return this.id;
		}

		public virtual long GetStartTime()
		{
			return this.startTime;
		}

		public virtual long GetFinishTime()
		{
			return this.finishTime;
		}

		public virtual float GetProgress()
		{
			return this.progress;
		}

		public virtual long GetElapsedTime()
		{
			return this.elapsedTime;
		}

		public virtual string GetNode()
		{
			return this.nodeHttpAddress;
		}

		public virtual string GetRack()
		{
			return this.rack;
		}

		public virtual string GetNote()
		{
			return this.diagnostics;
		}
	}
}
