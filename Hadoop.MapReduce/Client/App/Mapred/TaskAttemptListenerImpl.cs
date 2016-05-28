using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.App.Security.Authorize;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>This class is responsible for talking to the task umblical.</summary>
	/// <remarks>
	/// This class is responsible for talking to the task umblical.
	/// It also converts all the old data structures
	/// to yarn data structures.
	/// This class HAS to be in this package to access package private
	/// methods/classes.
	/// </remarks>
	public class TaskAttemptListenerImpl : CompositeService, TaskUmbilicalProtocol, TaskAttemptListener
	{
		private static readonly JvmTask TaskForInvalidJvm = new JvmTask(null, true);

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.TaskAttemptListenerImpl
			));

		private AppContext context;

		private Server server;

		protected internal TaskHeartbeatHandler taskHeartbeatHandler;

		private RMHeartbeatHandler rmHeartbeatHandler;

		private long commitWindowMs;

		private IPEndPoint address;

		private ConcurrentMap<WrappedJvmID, Task> jvmIDToActiveAttemptMap = new ConcurrentHashMap
			<WrappedJvmID, Task>();

		private ICollection<WrappedJvmID> launchedJVMs = Sharpen.Collections.NewSetFromMap
			(new ConcurrentHashMap<WrappedJvmID, bool>());

		private JobTokenSecretManager jobTokenSecretManager = null;

		private byte[] encryptedSpillKey;

		public TaskAttemptListenerImpl(AppContext context, JobTokenSecretManager jobTokenSecretManager
			, RMHeartbeatHandler rmHeartbeatHandler, byte[] secretShuffleKey)
			: base(typeof(Org.Apache.Hadoop.Mapred.TaskAttemptListenerImpl).FullName)
		{
			this.context = context;
			this.jobTokenSecretManager = jobTokenSecretManager;
			this.rmHeartbeatHandler = rmHeartbeatHandler;
			this.encryptedSpillKey = secretShuffleKey;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			RegisterHeartbeatHandler(conf);
			commitWindowMs = conf.GetLong(MRJobConfig.MrAmCommitWindowMs, MRJobConfig.DefaultMrAmCommitWindowMs
				);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			StartRpcServer();
			base.ServiceStart();
		}

		protected internal virtual void RegisterHeartbeatHandler(Configuration conf)
		{
			taskHeartbeatHandler = new TaskHeartbeatHandler(context.GetEventHandler(), context
				.GetClock(), conf.GetInt(MRJobConfig.MrAmTaskListenerThreadCount, MRJobConfig.DefaultMrAmTaskListenerThreadCount
				));
			AddService(taskHeartbeatHandler);
		}

		protected internal virtual void StartRpcServer()
		{
			Configuration conf = GetConfig();
			try
			{
				server = new RPC.Builder(conf).SetProtocol(typeof(TaskUmbilicalProtocol)).SetInstance
					(this).SetBindAddress("0.0.0.0").SetPort(0).SetNumHandlers(conf.GetInt(MRJobConfig
					.MrAmTaskListenerThreadCount, MRJobConfig.DefaultMrAmTaskListenerThreadCount)).SetVerbose
					(false).SetSecretManager(jobTokenSecretManager).Build();
				// Enable service authorization?
				if (conf.GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, false
					))
				{
					RefreshServiceAcls(conf, new MRAMPolicyProvider());
				}
				server.Start();
				this.address = NetUtils.CreateSocketAddrForHost(context.GetNMHostname(), server.GetListenerAddress
					().Port);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		internal virtual void RefreshServiceAcls(Configuration configuration, PolicyProvider
			 policyProvider)
		{
			this.server.RefreshServiceAcl(configuration, policyProvider);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			StopRpcServer();
			base.ServiceStop();
		}

		protected internal virtual void StopRpcServer()
		{
			if (server != null)
			{
				server.Stop();
			}
		}

		public virtual IPEndPoint GetAddress()
		{
			return address;
		}

		/// <summary>Child checking whether it can commit.</summary>
		/// <remarks>
		/// Child checking whether it can commit.
		/// <br />
		/// Commit is a two-phased protocol. First the attempt informs the
		/// ApplicationMaster that it is
		/// <see cref="CommitPending(TaskAttemptID, TaskStatus)"/>
		/// . Then it repeatedly polls
		/// the ApplicationMaster whether it
		/// <see cref="CanCommit(TaskAttemptID)"/>
		/// This is
		/// a legacy from the centralized commit protocol handling by the JobTracker.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool CanCommit(TaskAttemptID taskAttemptID)
		{
			Log.Info("Commit go/no-go request from " + taskAttemptID.ToString());
			// An attempt is asking if it can commit its output. This can be decided
			// only by the task which is managing the multiple attempts. So redirect the
			// request there.
			TaskAttemptId attemptID = TypeConverter.ToYarn(taskAttemptID);
			taskHeartbeatHandler.Progressing(attemptID);
			// tell task to retry later if AM has not heard from RM within the commit
			// window to help avoid double-committing in a split-brain situation
			long now = context.GetClock().GetTime();
			if (now - rmHeartbeatHandler.GetLastHeartbeatTime() > commitWindowMs)
			{
				return false;
			}
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = context.GetJob(attemptID.GetTaskId
				().GetJobId());
			Task task = job.GetTask(attemptID.GetTaskId());
			return task.CanCommit(attemptID);
		}

		/// <summary>
		/// TaskAttempt is reporting that it is in commit_pending and it is waiting for
		/// the commit Response
		/// <br />
		/// Commit it a two-phased protocol.
		/// </summary>
		/// <remarks>
		/// TaskAttempt is reporting that it is in commit_pending and it is waiting for
		/// the commit Response
		/// <br />
		/// Commit it a two-phased protocol. First the attempt informs the
		/// ApplicationMaster that it is
		/// <see cref="CommitPending(TaskAttemptID, TaskStatus)"/>
		/// . Then it repeatedly polls
		/// the ApplicationMaster whether it
		/// <see cref="CanCommit(TaskAttemptID)"/>
		/// This is
		/// a legacy from the centralized commit protocol handling by the JobTracker.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void CommitPending(TaskAttemptID taskAttemptID, TaskStatus taskStatsu
			)
		{
			Log.Info("Commit-pending state update from " + taskAttemptID.ToString());
			// An attempt is asking if it can commit its output. This can be decided
			// only by the task which is managing the multiple attempts. So redirect the
			// request there.
			TaskAttemptId attemptID = TypeConverter.ToYarn(taskAttemptID);
			taskHeartbeatHandler.Progressing(attemptID);
			//Ignorable TaskStatus? - since a task will send a LastStatusUpdate
			context.GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
				.TaCommitPending));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Done(TaskAttemptID taskAttemptID)
		{
			Log.Info("Done acknowledgement from " + taskAttemptID.ToString());
			TaskAttemptId attemptID = TypeConverter.ToYarn(taskAttemptID);
			taskHeartbeatHandler.Progressing(attemptID);
			context.GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
				.TaDone));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void FatalError(TaskAttemptID taskAttemptID, string msg)
		{
			// This happens only in Child and in the Task.
			Log.Fatal("Task: " + taskAttemptID + " - exited : " + msg);
			ReportDiagnosticInfo(taskAttemptID, "Error: " + msg);
			TaskAttemptId attemptID = TypeConverter.ToYarn(taskAttemptID);
			context.GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
				.TaFailmsg));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void FsError(TaskAttemptID taskAttemptID, string message)
		{
			// This happens only in Child.
			Log.Fatal("Task: " + taskAttemptID + " - failed due to FSError: " + message);
			ReportDiagnosticInfo(taskAttemptID, "FSError: " + message);
			TaskAttemptId attemptID = TypeConverter.ToYarn(taskAttemptID);
			context.GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
				.TaFailmsg));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ShuffleError(TaskAttemptID taskAttemptID, string message)
		{
		}

		// TODO: This isn't really used in any MR code. Ask for removal.    
		/// <exception cref="System.IO.IOException"/>
		public virtual MapTaskCompletionEventsUpdate GetMapCompletionEvents(JobID jobIdentifier
			, int startIndex, int maxEvents, TaskAttemptID taskAttemptID)
		{
			Log.Info("MapCompletionEvents request from " + taskAttemptID.ToString() + ". startIndex "
				 + startIndex + " maxEvents " + maxEvents);
			// TODO: shouldReset is never used. See TT. Ask for Removal.
			bool shouldReset = false;
			TaskAttemptId attemptID = TypeConverter.ToYarn(taskAttemptID);
			TaskCompletionEvent[] events = context.GetJob(attemptID.GetTaskId().GetJobId()).GetMapAttemptCompletionEvents
				(startIndex, maxEvents);
			taskHeartbeatHandler.Progressing(attemptID);
			return new MapTaskCompletionEventsUpdate(events, shouldReset);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Ping(TaskAttemptID taskAttemptID)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Ping from " + taskAttemptID.ToString());
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReportDiagnosticInfo(TaskAttemptID taskAttemptID, string diagnosticInfo
			)
		{
			diagnosticInfo = StringInterner.WeakIntern(diagnosticInfo);
			Log.Info("Diagnostics report from " + taskAttemptID.ToString() + ": " + diagnosticInfo
				);
			TaskAttemptId attemptID = TypeConverter.ToYarn(taskAttemptID);
			taskHeartbeatHandler.Progressing(attemptID);
			// This is mainly used for cases where we want to propagate exception traces
			// of tasks that fail.
			// This call exists as a hadoop mapreduce legacy wherein all changes in
			// counters/progress/phase/output-size are reported through statusUpdate()
			// call but not diagnosticInformation.
			context.GetEventHandler().Handle(new TaskAttemptDiagnosticsUpdateEvent(attemptID, 
				diagnosticInfo));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual bool StatusUpdate(TaskAttemptID taskAttemptID, TaskStatus taskStatus
			)
		{
			TaskAttemptId yarnAttemptID = TypeConverter.ToYarn(taskAttemptID);
			taskHeartbeatHandler.Progressing(yarnAttemptID);
			TaskAttemptStatusUpdateEvent.TaskAttemptStatus taskAttemptStatus = new TaskAttemptStatusUpdateEvent.TaskAttemptStatus
				();
			taskAttemptStatus.id = yarnAttemptID;
			// Task sends the updated progress to the TT.
			taskAttemptStatus.progress = taskStatus.GetProgress();
			Log.Info("Progress of TaskAttempt " + taskAttemptID + " is : " + taskStatus.GetProgress
				());
			// Task sends the updated state-string to the TT.
			taskAttemptStatus.stateString = taskStatus.GetStateString();
			// Task sends the updated phase to the TT.
			taskAttemptStatus.phase = TypeConverter.ToYarn(taskStatus.GetPhase());
			// Counters are updated by the task. Convert counters into new format as
			// that is the primary storage format inside the AM to avoid multiple
			// conversions and unnecessary heap usage.
			taskAttemptStatus.counters = new Counters(taskStatus.GetCounters());
			// Map Finish time set by the task (map only)
			if (taskStatus.GetIsMap() && taskStatus.GetMapFinishTime() != 0)
			{
				taskAttemptStatus.mapFinishTime = taskStatus.GetMapFinishTime();
			}
			// Shuffle Finish time set by the task (reduce only).
			if (!taskStatus.GetIsMap() && taskStatus.GetShuffleFinishTime() != 0)
			{
				taskAttemptStatus.shuffleFinishTime = taskStatus.GetShuffleFinishTime();
			}
			// Sort finish time set by the task (reduce only).
			if (!taskStatus.GetIsMap() && taskStatus.GetSortFinishTime() != 0)
			{
				taskAttemptStatus.sortFinishTime = taskStatus.GetSortFinishTime();
			}
			// Not Setting the task state. Used by speculation - will be set in TaskAttemptImpl
			//taskAttemptStatus.taskState =  TypeConverter.toYarn(taskStatus.getRunState());
			//set the fetch failures
			if (taskStatus.GetFetchFailedMaps() != null && taskStatus.GetFetchFailedMaps().Count
				 > 0)
			{
				taskAttemptStatus.fetchFailedMaps = new AList<TaskAttemptId>();
				foreach (TaskAttemptID failedMapId in taskStatus.GetFetchFailedMaps())
				{
					taskAttemptStatus.fetchFailedMaps.AddItem(TypeConverter.ToYarn(failedMapId));
				}
			}
			// Task sends the information about the nextRecordRange to the TT
			//    TODO: The following are not needed here, but needed to be set somewhere inside AppMaster.
			//    taskStatus.getRunState(); // Set by the TT/JT. Transform into a state TODO
			//    taskStatus.getStartTime(); // Used to be set by the TaskTracker. This should be set by getTask().
			//    taskStatus.getFinishTime(); // Used to be set by TT/JT. Should be set when task finishes
			//    // This was used by TT to do counter updates only once every minute. So this
			//    // isn't ever changed by the Task itself.
			//    taskStatus.getIncludeCounters();
			context.GetEventHandler().Handle(new TaskAttemptStatusUpdateEvent(taskAttemptStatus
				.id, taskAttemptStatus));
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetProtocolVersion(string arg0, long arg1)
		{
			return TaskUmbilicalProtocol.versionID;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReportNextRecordRange(TaskAttemptID taskAttemptID, SortedRanges.Range
			 range)
		{
			// This is used when the feature of skipping records is enabled.
			// This call exists as a hadoop mapreduce legacy wherein all changes in
			// counters/progress/phase/output-size are reported through statusUpdate()
			// call but not the next record range information.
			throw new IOException("Not yet implemented.");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual JvmTask GetTask(JvmContext context)
		{
			// A rough imitation of code from TaskTracker.
			JVMId jvmId = context.jvmId;
			Log.Info("JVM with ID : " + jvmId + " asked for a task");
			JvmTask jvmTask = null;
			// TODO: Is it an authorized container to get a task? Otherwise return null.
			// TODO: Child.java's firstTaskID isn't really firstTaskID. Ask for update
			// to jobId and task-type.
			WrappedJvmID wJvmID = new WrappedJvmID(jvmId.GetJobId(), jvmId.isMap, jvmId.GetId
				());
			// Try to look up the task. We remove it directly as we don't give
			// multiple tasks to a JVM
			if (!jvmIDToActiveAttemptMap.Contains(wJvmID))
			{
				Log.Info("JVM with ID: " + jvmId + " is invalid and will be killed.");
				jvmTask = TaskForInvalidJvm;
			}
			else
			{
				if (!launchedJVMs.Contains(wJvmID))
				{
					jvmTask = null;
					Log.Info("JVM with ID: " + jvmId + " asking for task before AM launch registered. Given null task"
						);
				}
				else
				{
					// remove the task as it is no more needed and free up the memory.
					// Also we have already told the JVM to process a task, so it is no
					// longer pending, and further request should ask it to exit.
					Task task = Sharpen.Collections.Remove(jvmIDToActiveAttemptMap, wJvmID);
					launchedJVMs.Remove(wJvmID);
					Log.Info("JVM with ID: " + jvmId + " given task: " + task.GetTaskID());
					task.SetEncryptedSpillKey(encryptedSpillKey);
					jvmTask = new JvmTask(task, false);
				}
			}
			return jvmTask;
		}

		public virtual void RegisterPendingTask(Task task, WrappedJvmID jvmID)
		{
			// Create the mapping so that it is easy to look up
			// when the jvm comes back to ask for Task.
			// A JVM not present in this map is an illegal task/JVM.
			jvmIDToActiveAttemptMap[jvmID] = task;
		}

		public virtual void RegisterLaunchedTask(TaskAttemptId attemptID, WrappedJvmID jvmId
			)
		{
			// The AM considers the task to be launched (Has asked the NM to launch it)
			// The JVM will only be given a task after this registartion.
			launchedJVMs.AddItem(jvmId);
			taskHeartbeatHandler.Register(attemptID);
		}

		public virtual void Unregister(TaskAttemptId attemptID, WrappedJvmID jvmID)
		{
			// Unregistration also comes from the same TaskAttempt which does the
			// registration. Events are ordered at TaskAttempt, so unregistration will
			// always come after registration.
			// Remove from launchedJVMs before jvmIDToActiveAttemptMap to avoid
			// synchronization issue with getTask(). getTask should be checking
			// jvmIDToActiveAttemptMap before it checks launchedJVMs.
			// remove the mappings if not already removed
			launchedJVMs.Remove(jvmID);
			Sharpen.Collections.Remove(jvmIDToActiveAttemptMap, jvmID);
			//unregister this attempt
			taskHeartbeatHandler.Unregister(attemptID);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
			, int clientMethodsHash)
		{
			return ProtocolSignature.GetProtocolSignature(this, protocol, clientVersion, clientMethodsHash
				);
		}
	}
}
