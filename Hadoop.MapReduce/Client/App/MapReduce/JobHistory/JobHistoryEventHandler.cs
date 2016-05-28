using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Codehaus.Jackson;
using Org.Codehaus.Jackson.Map;
using Org.Codehaus.Jackson.Node;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>The job history events get routed to this class.</summary>
	/// <remarks>
	/// The job history events get routed to this class. This class writes the Job
	/// history events to the DFS directly into a staging dir and then moved to a
	/// done-dir. JobHistory implementation is in this package to access package
	/// private classes.
	/// </remarks>
	public class JobHistoryEventHandler : AbstractService, EventHandler<JobHistoryEvent
		>
	{
		private readonly AppContext context;

		private readonly int startCount;

		private int eventCounter;

		private FileSystem stagingDirFS;

		private FileSystem doneDirFS;

		private Path stagingDirPath = null;

		private Path doneDirPrefixPath = null;

		private int maxUnflushedCompletionEvents;

		private int postJobCompletionMultiplier;

		private long flushTimeout;

		private int minQueueSizeForBatchingFlushes;

		private int numUnflushedCompletionEvents = 0;

		private bool isTimerActive;

		protected internal BlockingQueue<JobHistoryEvent> eventQueue = new LinkedBlockingQueue
			<JobHistoryEvent>();

		protected internal Sharpen.Thread eventHandlingThread;

		private volatile bool stopped;

		private readonly object Lock = new object();

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Jobhistory.JobHistoryEventHandler
			));

		protected internal static readonly IDictionary<JobId, JobHistoryEventHandler.MetaInfo
			> fileMap = Sharpen.Collections.SynchronizedMap<JobId, JobHistoryEventHandler.MetaInfo
			>(new Dictionary<JobId, JobHistoryEventHandler.MetaInfo>());

		protected internal volatile bool forceJobCompletion = false;

		protected internal TimelineClient timelineClient;

		private static string MapreduceJobEntityType = "MAPREDUCE_JOB";

		private static string MapreduceTaskEntityType = "MAPREDUCE_TASK";

		public JobHistoryEventHandler(AppContext context, int startCount)
			: base("JobHistoryEventHandler")
		{
			// Those file systems may differ from the job configuration
			// See org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils
			// #ensurePathInDefaultFileSystem
			// log Dir FileSystem
			// done Dir FileSystem
			// folder for completed jobs
			// TODO: Rename
			// should job completion be force when the AM shuts down?
			this.context = context;
			this.startCount = startCount;
		}

		/* (non-Javadoc)
		* @see org.apache.hadoop.yarn.service.AbstractService#init(org.
		* apache.hadoop.conf.Configuration)
		* Initializes the FileSystem and Path objects for the log and done directories.
		* Creates these directories if they do not already exist.
		*/
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			string jobId = TypeConverter.FromYarn(context.GetApplicationID()).ToString();
			string stagingDirStr = null;
			string doneDirStr = null;
			string userDoneDirStr = null;
			try
			{
				stagingDirStr = JobHistoryUtils.GetConfiguredHistoryStagingDirPrefix(conf, jobId);
				doneDirStr = JobHistoryUtils.GetConfiguredHistoryIntermediateDoneDirPrefix(conf);
				userDoneDirStr = JobHistoryUtils.GetHistoryIntermediateDoneDirForUser(conf);
			}
			catch (IOException e)
			{
				Log.Error("Failed while getting the configured log directories", e);
				throw new YarnRuntimeException(e);
			}
			//Check for the existence of the history staging dir. Maybe create it. 
			try
			{
				stagingDirPath = FileContext.GetFileContext(conf).MakeQualified(new Path(stagingDirStr
					));
				stagingDirFS = FileSystem.Get(stagingDirPath.ToUri(), conf);
				Mkdir(stagingDirFS, stagingDirPath, new FsPermission(JobHistoryUtils.HistoryStagingDirPermissions
					));
			}
			catch (IOException e)
			{
				Log.Error("Failed while checking for/creating  history staging path: [" + stagingDirPath
					 + "]", e);
				throw new YarnRuntimeException(e);
			}
			//Check for the existence of intermediate done dir.
			Path doneDirPath = null;
			try
			{
				doneDirPath = FileContext.GetFileContext(conf).MakeQualified(new Path(doneDirStr)
					);
				doneDirFS = FileSystem.Get(doneDirPath.ToUri(), conf);
				// This directory will be in a common location, or this may be a cluster
				// meant for a single user. Creating based on the conf. Should ideally be
				// created by the JobHistoryServer or as part of deployment.
				if (!doneDirFS.Exists(doneDirPath))
				{
					if (JobHistoryUtils.ShouldCreateNonUserDirectory(conf))
					{
						Log.Info("Creating intermediate history logDir: [" + doneDirPath + "] + based on conf. Should ideally be created by the JobHistoryServer: "
							 + MRJobConfig.MrAmCreateJhIntermediateBaseDir);
						Mkdir(doneDirFS, doneDirPath, new FsPermission(JobHistoryUtils.HistoryIntermediateDoneDirPermissions
							.ToShort()));
					}
					else
					{
						// TODO Temporary toShort till new FsPermission(FsPermissions)
						// respects
						// sticky
						string message = "Not creating intermediate history logDir: [" + doneDirPath + "] based on conf: "
							 + MRJobConfig.MrAmCreateJhIntermediateBaseDir + ". Either set to true or pre-create this directory with"
							 + " appropriate permissions";
						Log.Error(message);
						throw new YarnRuntimeException(message);
					}
				}
			}
			catch (IOException e)
			{
				Log.Error("Failed checking for the existance of history intermediate " + "done directory: ["
					 + doneDirPath + "]");
				throw new YarnRuntimeException(e);
			}
			//Check/create user directory under intermediate done dir.
			try
			{
				doneDirPrefixPath = FileContext.GetFileContext(conf).MakeQualified(new Path(userDoneDirStr
					));
				Mkdir(doneDirFS, doneDirPrefixPath, new FsPermission(JobHistoryUtils.HistoryIntermediateUserDirPermissions
					));
			}
			catch (IOException e)
			{
				Log.Error("Error creating user intermediate history done directory: [ " + doneDirPrefixPath
					 + "]", e);
				throw new YarnRuntimeException(e);
			}
			// Maximum number of unflushed completion-events that can stay in the queue
			// before flush kicks in.
			maxUnflushedCompletionEvents = conf.GetInt(MRJobConfig.MrAmHistoryMaxUnflushedCompleteEvents
				, MRJobConfig.DefaultMrAmHistoryMaxUnflushedCompleteEvents);
			// We want to cut down flushes after job completes so as to write quicker,
			// so we increase maxUnflushedEvents post Job completion by using the
			// following multiplier.
			postJobCompletionMultiplier = conf.GetInt(MRJobConfig.MrAmHistoryJobCompleteUnflushedMultiplier
				, MRJobConfig.DefaultMrAmHistoryJobCompleteUnflushedMultiplier);
			// Max time until which flush doesn't take place.
			flushTimeout = conf.GetLong(MRJobConfig.MrAmHistoryCompleteEventFlushTimeoutMs, MRJobConfig
				.DefaultMrAmHistoryCompleteEventFlushTimeoutMs);
			minQueueSizeForBatchingFlushes = conf.GetInt(MRJobConfig.MrAmHistoryUseBatchedFlushQueueSizeThreshold
				, MRJobConfig.DefaultMrAmHistoryUseBatchedFlushQueueSizeThreshold);
			if (conf.GetBoolean(MRJobConfig.MapreduceJobEmitTimelineData, MRJobConfig.DefaultMapreduceJobEmitTimelineData
				))
			{
				if (conf.GetBoolean(YarnConfiguration.TimelineServiceEnabled, YarnConfiguration.DefaultTimelineServiceEnabled
					))
				{
					timelineClient = TimelineClient.CreateTimelineClient();
					timelineClient.Init(conf);
					Log.Info("Timeline service is enabled");
					Log.Info("Emitting job history data to the timeline server is enabled");
				}
				else
				{
					Log.Info("Timeline service is not enabled");
				}
			}
			else
			{
				Log.Info("Emitting job history data to the timeline server is not enabled");
			}
			base.ServiceInit(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void Mkdir(FileSystem fs, Path path, FsPermission fsp)
		{
			if (!fs.Exists(path))
			{
				try
				{
					fs.Mkdirs(path, fsp);
					FileStatus fsStatus = fs.GetFileStatus(path);
					Log.Info("Perms after creating " + fsStatus.GetPermission().ToShort() + ", Expected: "
						 + fsp.ToShort());
					if (fsStatus.GetPermission().ToShort() != fsp.ToShort())
					{
						Log.Info("Explicitly setting permissions to : " + fsp.ToShort() + ", " + fsp);
						fs.SetPermission(path, fsp);
					}
				}
				catch (FileAlreadyExistsException)
				{
					Log.Info("Directory: [" + path + "] already exists.");
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			if (timelineClient != null)
			{
				timelineClient.Start();
			}
			eventHandlingThread = new Sharpen.Thread(new _Runnable_289(this), "eventHandlingThread"
				);
			// Log the size of the history-event-queue every so often.
			// If an event has been removed from the queue. Handle it.
			// The rest of the queue is handled via stop()
			// Clear the interrupt status if it's set before calling handleEvent
			// and set it if it was set before calling handleEvent. 
			// Interrupts received from other threads during handleEvent cannot be
			// dealth with - Shell.runCommand() ignores them.
			eventHandlingThread.Start();
			base.ServiceStart();
		}

		private sealed class _Runnable_289 : Runnable
		{
			public _Runnable_289(JobHistoryEventHandler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				JobHistoryEvent @event = null;
				while (!this._enclosing.stopped && !Sharpen.Thread.CurrentThread().IsInterrupted(
					))
				{
					if (this._enclosing.eventCounter != 0 && this._enclosing.eventCounter % 1000 == 0)
					{
						this._enclosing.eventCounter = 0;
						Org.Apache.Hadoop.Mapreduce.Jobhistory.JobHistoryEventHandler.Log.Info("Size of the JobHistory event queue is "
							 + this._enclosing.eventQueue.Count);
					}
					else
					{
						this._enclosing.eventCounter++;
					}
					try
					{
						@event = this._enclosing.eventQueue.Take();
					}
					catch (Exception)
					{
						Org.Apache.Hadoop.Mapreduce.Jobhistory.JobHistoryEventHandler.Log.Info("EventQueue take interrupted. Returning"
							);
						return;
					}
					lock (this._enclosing.Lock)
					{
						bool isInterrupted = Sharpen.Thread.Interrupted();
						this._enclosing.HandleEvent(@event);
						if (isInterrupted)
						{
							Org.Apache.Hadoop.Mapreduce.Jobhistory.JobHistoryEventHandler.Log.Debug("Event handling interrupted"
								);
							Sharpen.Thread.CurrentThread().Interrupt();
						}
					}
				}
			}

			private readonly JobHistoryEventHandler _enclosing;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			Log.Info("Stopping JobHistoryEventHandler. " + "Size of the outstanding queue size is "
				 + eventQueue.Count);
			stopped = true;
			//do not interrupt while event handling is in progress
			lock (Lock)
			{
				if (eventHandlingThread != null)
				{
					Log.Debug("Interrupting Event Handling thread");
					eventHandlingThread.Interrupt();
				}
				else
				{
					Log.Debug("Null event handling thread");
				}
			}
			try
			{
				if (eventHandlingThread != null)
				{
					Log.Debug("Waiting for Event Handling thread to complete");
					eventHandlingThread.Join();
				}
			}
			catch (Exception ie)
			{
				Log.Info("Interrupted Exception while stopping", ie);
			}
			// Cancel all timers - so that they aren't invoked during or after
			// the metaInfo object is wrapped up.
			foreach (JobHistoryEventHandler.MetaInfo mi in fileMap.Values)
			{
				try
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Shutting down timer for " + mi);
					}
					mi.ShutDownTimer();
				}
				catch (IOException e)
				{
					Log.Info("Exception while cancelling delayed flush timer. " + "Likely caused by a failed flush "
						 + e.Message);
				}
			}
			//write all the events remaining in queue
			IEnumerator<JobHistoryEvent> it = eventQueue.GetEnumerator();
			while (it.HasNext())
			{
				JobHistoryEvent ev = it.Next();
				Log.Info("In stop, writing event " + ev.GetType());
				HandleEvent(ev);
			}
			// Process JobUnsuccessfulCompletionEvent for jobIds which still haven't
			// closed their event writers
			if (forceJobCompletion)
			{
				foreach (KeyValuePair<JobId, JobHistoryEventHandler.MetaInfo> jobIt in fileMap)
				{
					JobId toClose = jobIt.Key;
					JobHistoryEventHandler.MetaInfo mi_1 = jobIt.Value;
					if (mi_1 != null && mi_1.IsWriterActive())
					{
						Log.Warn("Found jobId " + toClose + " to have not been closed. Will close");
						//Create a JobFinishEvent so that it is written to the job history
						Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = context.GetJob(toClose);
						JobUnsuccessfulCompletionEvent jucEvent = new JobUnsuccessfulCompletionEvent(TypeConverter
							.FromYarn(toClose), Runtime.CurrentTimeMillis(), job.GetCompletedMaps(), job.GetCompletedReduces
							(), CreateJobStateForJobUnsuccessfulCompletionEvent(mi_1.GetForcedJobStateOnShutDown
							()), job.GetDiagnostics());
						JobHistoryEvent jfEvent = new JobHistoryEvent(toClose, jucEvent);
						//Bypass the queue mechanism which might wait. Call the method directly
						HandleEvent(jfEvent);
					}
				}
			}
			//close all file handles
			foreach (JobHistoryEventHandler.MetaInfo mi_2 in fileMap.Values)
			{
				try
				{
					mi_2.CloseWriter();
				}
				catch (IOException e)
				{
					Log.Info("Exception while closing file " + e.Message);
				}
			}
			if (timelineClient != null)
			{
				timelineClient.Stop();
			}
			Log.Info("Stopped JobHistoryEventHandler. super.stop()");
			base.ServiceStop();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual EventWriter CreateEventWriter(Path historyFilePath)
		{
			FSDataOutputStream @out = stagingDirFS.Create(historyFilePath, true);
			return new EventWriter(@out);
		}

		/// <summary>Create an event writer for the Job represented by the jobID.</summary>
		/// <remarks>
		/// Create an event writer for the Job represented by the jobID.
		/// Writes out the job configuration to the log directory.
		/// This should be the first call to history for a job
		/// </remarks>
		/// <param name="jobId">the jobId.</param>
		/// <param name="amStartedEvent"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void SetupEventWriter(JobId jobId, AMStartedEvent amStartedEvent
			)
		{
			if (stagingDirPath == null)
			{
				Log.Error("Log Directory is null, returning");
				throw new IOException("Missing Log Directory for History");
			}
			JobHistoryEventHandler.MetaInfo oldFi = fileMap[jobId];
			Configuration conf = GetConfig();
			// TODO Ideally this should be written out to the job dir
			// (.staging/jobid/files - RecoveryService will need to be patched)
			Path historyFile = JobHistoryUtils.GetStagingJobHistoryFile(stagingDirPath, jobId
				, startCount);
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			if (user == null)
			{
				throw new IOException("User is null while setting up jobhistory eventwriter");
			}
			string jobName = context.GetJob(jobId).GetName();
			EventWriter writer = (oldFi == null) ? null : oldFi.writer;
			Path logDirConfPath = JobHistoryUtils.GetStagingConfFile(stagingDirPath, jobId, startCount
				);
			if (writer == null)
			{
				try
				{
					writer = CreateEventWriter(historyFile);
					Log.Info("Event Writer setup for JobId: " + jobId + ", File: " + historyFile);
				}
				catch (IOException ioe)
				{
					Log.Info("Could not create log file: [" + historyFile + "] + for job " + "[" + jobName
						 + "]");
					throw;
				}
				//Write out conf only if the writer isn't already setup.
				if (conf != null)
				{
					// TODO Ideally this should be written out to the job dir
					// (.staging/jobid/files - RecoveryService will need to be patched)
					FSDataOutputStream jobFileOut = null;
					try
					{
						if (logDirConfPath != null)
						{
							jobFileOut = stagingDirFS.Create(logDirConfPath, true);
							conf.WriteXml(jobFileOut);
							jobFileOut.Close();
						}
					}
					catch (IOException e)
					{
						Log.Info("Failed to write the job configuration file", e);
						throw;
					}
				}
			}
			string queueName = JobConf.DefaultQueueName;
			if (conf != null)
			{
				queueName = conf.Get(MRJobConfig.QueueName, JobConf.DefaultQueueName);
			}
			JobHistoryEventHandler.MetaInfo fi = new JobHistoryEventHandler.MetaInfo(this, historyFile
				, logDirConfPath, writer, user, jobName, jobId, amStartedEvent.GetForcedJobStateOnShutDown
				(), queueName);
			fi.GetJobSummary().SetJobId(jobId);
			fi.GetJobSummary().SetJobLaunchTime(amStartedEvent.GetStartTime());
			fi.GetJobSummary().SetJobSubmitTime(amStartedEvent.GetSubmitTime());
			fi.GetJobIndexInfo().SetJobStartTime(amStartedEvent.GetStartTime());
			fi.GetJobIndexInfo().SetSubmitTime(amStartedEvent.GetSubmitTime());
			fileMap[jobId] = fi;
		}

		/// <summary>Close the event writer for this id</summary>
		/// <exception cref="System.IO.IOException"></exception>
		public virtual void CloseWriter(JobId id)
		{
			try
			{
				JobHistoryEventHandler.MetaInfo mi = fileMap[id];
				if (mi != null)
				{
					mi.CloseWriter();
				}
			}
			catch (IOException e)
			{
				Log.Error("Error closing writer for JobID: " + id);
				throw;
			}
		}

		public virtual void Handle(JobHistoryEvent @event)
		{
			try
			{
				if (IsJobCompletionEvent(@event.GetHistoryEvent()))
				{
					// When the job is complete, flush slower but write faster.
					maxUnflushedCompletionEvents = maxUnflushedCompletionEvents * postJobCompletionMultiplier;
				}
				eventQueue.Put(@event);
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		private bool IsJobCompletionEvent(HistoryEvent historyEvent)
		{
			if (EnumSet.Of(EventType.JobFinished, EventType.JobFailed, EventType.JobKilled).Contains
				(historyEvent.GetEventType()))
			{
				return true;
			}
			return false;
		}

		[InterfaceAudience.Private]
		public virtual void HandleEvent(JobHistoryEvent @event)
		{
			lock (Lock)
			{
				// If this is JobSubmitted Event, setup the writer
				if (@event.GetHistoryEvent().GetEventType() == EventType.AmStarted)
				{
					try
					{
						AMStartedEvent amStartedEvent = (AMStartedEvent)@event.GetHistoryEvent();
						SetupEventWriter(@event.GetJobID(), amStartedEvent);
					}
					catch (IOException ioe)
					{
						Log.Error("Error JobHistoryEventHandler in handleEvent: " + @event, ioe);
						throw new YarnRuntimeException(ioe);
					}
				}
				// For all events
				// (1) Write it out
				// (2) Process it for JobSummary
				// (3) Process it for ATS (if enabled)
				JobHistoryEventHandler.MetaInfo mi = fileMap[@event.GetJobID()];
				try
				{
					HistoryEvent historyEvent = @event.GetHistoryEvent();
					if (!(historyEvent is NormalizedResourceEvent))
					{
						mi.WriteEvent(historyEvent);
					}
					ProcessEventForJobSummary(@event.GetHistoryEvent(), mi.GetJobSummary(), @event.GetJobID
						());
					if (timelineClient != null)
					{
						ProcessEventForTimelineServer(historyEvent, @event.GetJobID(), @event.GetTimestamp
							());
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("In HistoryEventHandler " + @event.GetHistoryEvent().GetEventType());
					}
				}
				catch (IOException e)
				{
					Log.Error("Error writing History Event: " + @event.GetHistoryEvent(), e);
					throw new YarnRuntimeException(e);
				}
				if (@event.GetHistoryEvent().GetEventType() == EventType.JobSubmitted)
				{
					JobSubmittedEvent jobSubmittedEvent = (JobSubmittedEvent)@event.GetHistoryEvent();
					mi.GetJobIndexInfo().SetSubmitTime(jobSubmittedEvent.GetSubmitTime());
					mi.GetJobIndexInfo().SetQueueName(jobSubmittedEvent.GetJobQueueName());
				}
				//initialize the launchTime in the JobIndexInfo of MetaInfo
				if (@event.GetHistoryEvent().GetEventType() == EventType.JobInited)
				{
					JobInitedEvent jie = (JobInitedEvent)@event.GetHistoryEvent();
					mi.GetJobIndexInfo().SetJobStartTime(jie.GetLaunchTime());
				}
				if (@event.GetHistoryEvent().GetEventType() == EventType.JobQueueChanged)
				{
					JobQueueChangeEvent jQueueEvent = (JobQueueChangeEvent)@event.GetHistoryEvent();
					mi.GetJobIndexInfo().SetQueueName(jQueueEvent.GetJobQueueName());
				}
				// If this is JobFinishedEvent, close the writer and setup the job-index
				if (@event.GetHistoryEvent().GetEventType() == EventType.JobFinished)
				{
					try
					{
						JobFinishedEvent jFinishedEvent = (JobFinishedEvent)@event.GetHistoryEvent();
						mi.GetJobIndexInfo().SetFinishTime(jFinishedEvent.GetFinishTime());
						mi.GetJobIndexInfo().SetNumMaps(jFinishedEvent.GetFinishedMaps());
						mi.GetJobIndexInfo().SetNumReduces(jFinishedEvent.GetFinishedReduces());
						mi.GetJobIndexInfo().SetJobStatus(JobState.Succeeded.ToString());
						CloseEventWriter(@event.GetJobID());
						ProcessDoneFiles(@event.GetJobID());
					}
					catch (IOException e)
					{
						throw new YarnRuntimeException(e);
					}
				}
				// In case of JOB_ERROR, only process all the Done files(e.g. job
				// summary, job history file etc.) if it is last AM retry.
				if (@event.GetHistoryEvent().GetEventType() == EventType.JobError)
				{
					try
					{
						JobUnsuccessfulCompletionEvent jucEvent = (JobUnsuccessfulCompletionEvent)@event.
							GetHistoryEvent();
						mi.GetJobIndexInfo().SetFinishTime(jucEvent.GetFinishTime());
						mi.GetJobIndexInfo().SetNumMaps(jucEvent.GetFinishedMaps());
						mi.GetJobIndexInfo().SetNumReduces(jucEvent.GetFinishedReduces());
						mi.GetJobIndexInfo().SetJobStatus(jucEvent.GetStatus());
						CloseEventWriter(@event.GetJobID());
						if (context.IsLastAMRetry())
						{
							ProcessDoneFiles(@event.GetJobID());
						}
					}
					catch (IOException e)
					{
						throw new YarnRuntimeException(e);
					}
				}
				if (@event.GetHistoryEvent().GetEventType() == EventType.JobFailed || @event.GetHistoryEvent
					().GetEventType() == EventType.JobKilled)
				{
					try
					{
						JobUnsuccessfulCompletionEvent jucEvent = (JobUnsuccessfulCompletionEvent)@event.
							GetHistoryEvent();
						mi.GetJobIndexInfo().SetFinishTime(jucEvent.GetFinishTime());
						mi.GetJobIndexInfo().SetNumMaps(jucEvent.GetFinishedMaps());
						mi.GetJobIndexInfo().SetNumReduces(jucEvent.GetFinishedReduces());
						mi.GetJobIndexInfo().SetJobStatus(jucEvent.GetStatus());
						CloseEventWriter(@event.GetJobID());
						ProcessDoneFiles(@event.GetJobID());
					}
					catch (IOException e)
					{
						throw new YarnRuntimeException(e);
					}
				}
			}
		}

		public virtual void ProcessEventForJobSummary(HistoryEvent @event, JobSummary summary
			, JobId jobId)
		{
			switch (@event.GetEventType())
			{
				case EventType.JobSubmitted:
				{
					// context.getJob could be used for some of this info as well.
					JobSubmittedEvent jse = (JobSubmittedEvent)@event;
					summary.SetUser(jse.GetUserName());
					summary.SetQueue(jse.GetJobQueueName());
					summary.SetJobSubmitTime(jse.GetSubmitTime());
					summary.SetJobName(jse.GetJobName());
					break;
				}

				case EventType.NormalizedResource:
				{
					NormalizedResourceEvent normalizedResourceEvent = (NormalizedResourceEvent)@event;
					if (normalizedResourceEvent.GetTaskType() == TaskType.Map)
					{
						summary.SetResourcesPerMap(normalizedResourceEvent.GetMemory());
					}
					else
					{
						if (normalizedResourceEvent.GetTaskType() == TaskType.Reduce)
						{
							summary.SetResourcesPerReduce(normalizedResourceEvent.GetMemory());
						}
					}
					break;
				}

				case EventType.JobInited:
				{
					JobInitedEvent jie = (JobInitedEvent)@event;
					summary.SetJobLaunchTime(jie.GetLaunchTime());
					break;
				}

				case EventType.MapAttemptStarted:
				{
					TaskAttemptStartedEvent mtase = (TaskAttemptStartedEvent)@event;
					if (summary.GetFirstMapTaskLaunchTime() == 0)
					{
						summary.SetFirstMapTaskLaunchTime(mtase.GetStartTime());
					}
					break;
				}

				case EventType.ReduceAttemptStarted:
				{
					TaskAttemptStartedEvent rtase = (TaskAttemptStartedEvent)@event;
					if (summary.GetFirstReduceTaskLaunchTime() == 0)
					{
						summary.SetFirstReduceTaskLaunchTime(rtase.GetStartTime());
					}
					break;
				}

				case EventType.JobFinished:
				{
					JobFinishedEvent jfe = (JobFinishedEvent)@event;
					summary.SetJobFinishTime(jfe.GetFinishTime());
					summary.SetNumFinishedMaps(jfe.GetFinishedMaps());
					summary.SetNumFailedMaps(jfe.GetFailedMaps());
					summary.SetNumFinishedReduces(jfe.GetFinishedReduces());
					summary.SetNumFailedReduces(jfe.GetFailedReduces());
					if (summary.GetJobStatus() == null)
					{
						summary.SetJobStatus(JobStatus.State.Succeeded.ToString());
					}
					// TODO JOB_FINISHED does not have state. Effectively job history does not
					// have state about the finished job.
					SetSummarySlotSeconds(summary, jfe.GetTotalCounters());
					break;
				}

				case EventType.JobFailed:
				case EventType.JobKilled:
				{
					JobUnsuccessfulCompletionEvent juce = (JobUnsuccessfulCompletionEvent)@event;
					summary.SetJobStatus(juce.GetStatus());
					summary.SetNumFinishedMaps(context.GetJob(jobId).GetTotalMaps());
					summary.SetNumFinishedReduces(context.GetJob(jobId).GetTotalReduces());
					summary.SetJobFinishTime(juce.GetFinishTime());
					SetSummarySlotSeconds(summary, context.GetJob(jobId).GetAllCounters());
					break;
				}

				default:
				{
					break;
				}
			}
		}

		private void ProcessEventForTimelineServer(HistoryEvent @event, JobId jobId, long
			 timestamp)
		{
			TimelineEvent tEvent = new TimelineEvent();
			tEvent.SetEventType(StringUtils.ToUpperCase(@event.GetEventType().ToString()));
			tEvent.SetTimestamp(timestamp);
			TimelineEntity tEntity = new TimelineEntity();
			switch (@event.GetEventType())
			{
				case EventType.JobSubmitted:
				{
					JobSubmittedEvent jse = (JobSubmittedEvent)@event;
					tEvent.AddEventInfo("SUBMIT_TIME", jse.GetSubmitTime());
					tEvent.AddEventInfo("QUEUE_NAME", jse.GetJobQueueName());
					tEvent.AddEventInfo("JOB_NAME", jse.GetJobName());
					tEvent.AddEventInfo("USER_NAME", jse.GetUserName());
					tEvent.AddEventInfo("JOB_CONF_PATH", jse.GetJobConfPath());
					tEvent.AddEventInfo("ACLS", jse.GetJobAcls());
					tEvent.AddEventInfo("JOB_QUEUE_NAME", jse.GetJobQueueName());
					tEvent.AddEventInfo("WORKLFOW_ID", jse.GetWorkflowId());
					tEvent.AddEventInfo("WORKFLOW_NAME", jse.GetWorkflowName());
					tEvent.AddEventInfo("WORKFLOW_NAME_NAME", jse.GetWorkflowNodeName());
					tEvent.AddEventInfo("WORKFLOW_ADJACENCIES", jse.GetWorkflowAdjacencies());
					tEvent.AddEventInfo("WORKFLOW_TAGS", jse.GetWorkflowTags());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(jobId.ToString());
					tEntity.SetEntityType(MapreduceJobEntityType);
					break;
				}

				case EventType.JobStatusChanged:
				{
					JobStatusChangedEvent jsce = (JobStatusChangedEvent)@event;
					tEvent.AddEventInfo("STATUS", jsce.GetStatus());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(jobId.ToString());
					tEntity.SetEntityType(MapreduceJobEntityType);
					break;
				}

				case EventType.JobInfoChanged:
				{
					JobInfoChangeEvent jice = (JobInfoChangeEvent)@event;
					tEvent.AddEventInfo("SUBMIT_TIME", jice.GetSubmitTime());
					tEvent.AddEventInfo("LAUNCH_TIME", jice.GetLaunchTime());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(jobId.ToString());
					tEntity.SetEntityType(MapreduceJobEntityType);
					break;
				}

				case EventType.JobInited:
				{
					JobInitedEvent jie = (JobInitedEvent)@event;
					tEvent.AddEventInfo("START_TIME", jie.GetLaunchTime());
					tEvent.AddEventInfo("STATUS", jie.GetStatus());
					tEvent.AddEventInfo("TOTAL_MAPS", jie.GetTotalMaps());
					tEvent.AddEventInfo("TOTAL_REDUCES", jie.GetTotalReduces());
					tEvent.AddEventInfo("UBERIZED", jie.GetUberized());
					tEntity.SetStartTime(jie.GetLaunchTime());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(jobId.ToString());
					tEntity.SetEntityType(MapreduceJobEntityType);
					break;
				}

				case EventType.JobPriorityChanged:
				{
					JobPriorityChangeEvent jpce = (JobPriorityChangeEvent)@event;
					tEvent.AddEventInfo("PRIORITY", jpce.GetPriority().ToString());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(jobId.ToString());
					tEntity.SetEntityType(MapreduceJobEntityType);
					break;
				}

				case EventType.JobQueueChanged:
				{
					JobQueueChangeEvent jqe = (JobQueueChangeEvent)@event;
					tEvent.AddEventInfo("QUEUE_NAMES", jqe.GetJobQueueName());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(jobId.ToString());
					tEntity.SetEntityType(MapreduceJobEntityType);
					break;
				}

				case EventType.JobFailed:
				case EventType.JobKilled:
				case EventType.JobError:
				{
					JobUnsuccessfulCompletionEvent juce = (JobUnsuccessfulCompletionEvent)@event;
					tEvent.AddEventInfo("FINISH_TIME", juce.GetFinishTime());
					tEvent.AddEventInfo("NUM_MAPS", juce.GetFinishedMaps());
					tEvent.AddEventInfo("NUM_REDUCES", juce.GetFinishedReduces());
					tEvent.AddEventInfo("JOB_STATUS", juce.GetStatus());
					tEvent.AddEventInfo("DIAGNOSTICS", juce.GetDiagnostics());
					tEvent.AddEventInfo("FINISHED_MAPS", juce.GetFinishedMaps());
					tEvent.AddEventInfo("FINISHED_REDUCES", juce.GetFinishedReduces());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(jobId.ToString());
					tEntity.SetEntityType(MapreduceJobEntityType);
					break;
				}

				case EventType.JobFinished:
				{
					JobFinishedEvent jfe = (JobFinishedEvent)@event;
					tEvent.AddEventInfo("FINISH_TIME", jfe.GetFinishTime());
					tEvent.AddEventInfo("NUM_MAPS", jfe.GetFinishedMaps());
					tEvent.AddEventInfo("NUM_REDUCES", jfe.GetFinishedReduces());
					tEvent.AddEventInfo("FAILED_MAPS", jfe.GetFailedMaps());
					tEvent.AddEventInfo("FAILED_REDUCES", jfe.GetFailedReduces());
					tEvent.AddEventInfo("FINISHED_MAPS", jfe.GetFinishedMaps());
					tEvent.AddEventInfo("FINISHED_REDUCES", jfe.GetFinishedReduces());
					tEvent.AddEventInfo("MAP_COUNTERS_GROUPS", CountersToJSON(jfe.GetMapCounters()));
					tEvent.AddEventInfo("REDUCE_COUNTERS_GROUPS", CountersToJSON(jfe.GetReduceCounters
						()));
					tEvent.AddEventInfo("TOTAL_COUNTERS_GROUPS", CountersToJSON(jfe.GetTotalCounters(
						)));
					tEvent.AddEventInfo("JOB_STATUS", JobState.Succeeded.ToString());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(jobId.ToString());
					tEntity.SetEntityType(MapreduceJobEntityType);
					break;
				}

				case EventType.TaskStarted:
				{
					TaskStartedEvent tse = (TaskStartedEvent)@event;
					tEvent.AddEventInfo("TASK_TYPE", tse.GetTaskType().ToString());
					tEvent.AddEventInfo("START_TIME", tse.GetStartTime());
					tEvent.AddEventInfo("SPLIT_LOCATIONS", tse.GetSplitLocations());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(tse.GetTaskId().ToString());
					tEntity.SetEntityType(MapreduceTaskEntityType);
					tEntity.AddRelatedEntity(MapreduceJobEntityType, jobId.ToString());
					break;
				}

				case EventType.TaskFailed:
				{
					TaskFailedEvent tfe = (TaskFailedEvent)@event;
					tEvent.AddEventInfo("TASK_TYPE", tfe.GetTaskType().ToString());
					tEvent.AddEventInfo("STATUS", TaskStatus.State.Failed.ToString());
					tEvent.AddEventInfo("FINISH_TIME", tfe.GetFinishTime());
					tEvent.AddEventInfo("ERROR", tfe.GetError());
					tEvent.AddEventInfo("FAILED_ATTEMPT_ID", tfe.GetFailedAttemptID() == null ? string.Empty
						 : tfe.GetFailedAttemptID().ToString());
					tEvent.AddEventInfo("COUNTERS_GROUPS", CountersToJSON(tfe.GetCounters()));
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(tfe.GetTaskId().ToString());
					tEntity.SetEntityType(MapreduceTaskEntityType);
					tEntity.AddRelatedEntity(MapreduceJobEntityType, jobId.ToString());
					break;
				}

				case EventType.TaskUpdated:
				{
					TaskUpdatedEvent tue = (TaskUpdatedEvent)@event;
					tEvent.AddEventInfo("FINISH_TIME", tue.GetFinishTime());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(tue.GetTaskId().ToString());
					tEntity.SetEntityType(MapreduceTaskEntityType);
					tEntity.AddRelatedEntity(MapreduceJobEntityType, jobId.ToString());
					break;
				}

				case EventType.TaskFinished:
				{
					TaskFinishedEvent tfe2 = (TaskFinishedEvent)@event;
					tEvent.AddEventInfo("TASK_TYPE", tfe2.GetTaskType().ToString());
					tEvent.AddEventInfo("COUNTERS_GROUPS", CountersToJSON(tfe2.GetCounters()));
					tEvent.AddEventInfo("FINISH_TIME", tfe2.GetFinishTime());
					tEvent.AddEventInfo("STATUS", TaskStatus.State.Succeeded.ToString());
					tEvent.AddEventInfo("SUCCESSFUL_TASK_ATTEMPT_ID", tfe2.GetSuccessfulTaskAttemptId
						() == null ? string.Empty : tfe2.GetSuccessfulTaskAttemptId().ToString());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(tfe2.GetTaskId().ToString());
					tEntity.SetEntityType(MapreduceTaskEntityType);
					tEntity.AddRelatedEntity(MapreduceJobEntityType, jobId.ToString());
					break;
				}

				case EventType.MapAttemptStarted:
				case EventType.CleanupAttemptStarted:
				case EventType.ReduceAttemptStarted:
				case EventType.SetupAttemptStarted:
				{
					TaskAttemptStartedEvent tase = (TaskAttemptStartedEvent)@event;
					tEvent.AddEventInfo("TASK_TYPE", tase.GetTaskType().ToString());
					tEvent.AddEventInfo("TASK_ATTEMPT_ID", tase.GetTaskAttemptId().ToString());
					tEvent.AddEventInfo("START_TIME", tase.GetStartTime());
					tEvent.AddEventInfo("HTTP_PORT", tase.GetHttpPort());
					tEvent.AddEventInfo("TRACKER_NAME", tase.GetTrackerName());
					tEvent.AddEventInfo("TASK_TYPE", tase.GetTaskType().ToString());
					tEvent.AddEventInfo("SHUFFLE_PORT", tase.GetShufflePort());
					tEvent.AddEventInfo("CONTAINER_ID", tase.GetContainerId() == null ? string.Empty : 
						tase.GetContainerId().ToString());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(tase.GetTaskId().ToString());
					tEntity.SetEntityType(MapreduceTaskEntityType);
					tEntity.AddRelatedEntity(MapreduceJobEntityType, jobId.ToString());
					break;
				}

				case EventType.MapAttemptFailed:
				case EventType.CleanupAttemptFailed:
				case EventType.ReduceAttemptFailed:
				case EventType.SetupAttemptFailed:
				case EventType.MapAttemptKilled:
				case EventType.CleanupAttemptKilled:
				case EventType.ReduceAttemptKilled:
				case EventType.SetupAttemptKilled:
				{
					TaskAttemptUnsuccessfulCompletionEvent tauce = (TaskAttemptUnsuccessfulCompletionEvent
						)@event;
					tEvent.AddEventInfo("TASK_TYPE", tauce.GetTaskType().ToString());
					tEvent.AddEventInfo("TASK_ATTEMPT_ID", tauce.GetTaskAttemptId() == null ? string.Empty
						 : tauce.GetTaskAttemptId().ToString());
					tEvent.AddEventInfo("FINISH_TIME", tauce.GetFinishTime());
					tEvent.AddEventInfo("ERROR", tauce.GetError());
					tEvent.AddEventInfo("STATUS", tauce.GetTaskStatus());
					tEvent.AddEventInfo("HOSTNAME", tauce.GetHostname());
					tEvent.AddEventInfo("PORT", tauce.GetPort());
					tEvent.AddEventInfo("RACK_NAME", tauce.GetRackName());
					tEvent.AddEventInfo("SHUFFLE_FINISH_TIME", tauce.GetFinishTime());
					tEvent.AddEventInfo("SORT_FINISH_TIME", tauce.GetFinishTime());
					tEvent.AddEventInfo("MAP_FINISH_TIME", tauce.GetFinishTime());
					tEvent.AddEventInfo("COUNTERS_GROUPS", CountersToJSON(tauce.GetCounters()));
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(tauce.GetTaskId().ToString());
					tEntity.SetEntityType(MapreduceTaskEntityType);
					tEntity.AddRelatedEntity(MapreduceJobEntityType, jobId.ToString());
					break;
				}

				case EventType.MapAttemptFinished:
				{
					MapAttemptFinishedEvent mafe = (MapAttemptFinishedEvent)@event;
					tEvent.AddEventInfo("TASK_TYPE", mafe.GetTaskType().ToString());
					tEvent.AddEventInfo("FINISH_TIME", mafe.GetFinishTime());
					tEvent.AddEventInfo("STATUS", mafe.GetTaskStatus());
					tEvent.AddEventInfo("STATE", mafe.GetState());
					tEvent.AddEventInfo("MAP_FINISH_TIME", mafe.GetMapFinishTime());
					tEvent.AddEventInfo("COUNTERS_GROUPS", CountersToJSON(mafe.GetCounters()));
					tEvent.AddEventInfo("HOSTNAME", mafe.GetHostname());
					tEvent.AddEventInfo("PORT", mafe.GetPort());
					tEvent.AddEventInfo("RACK_NAME", mafe.GetRackName());
					tEvent.AddEventInfo("ATTEMPT_ID", mafe.GetAttemptId() == null ? string.Empty : mafe
						.GetAttemptId().ToString());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(mafe.GetTaskId().ToString());
					tEntity.SetEntityType(MapreduceTaskEntityType);
					tEntity.AddRelatedEntity(MapreduceJobEntityType, jobId.ToString());
					break;
				}

				case EventType.ReduceAttemptFinished:
				{
					ReduceAttemptFinishedEvent rafe = (ReduceAttemptFinishedEvent)@event;
					tEvent.AddEventInfo("TASK_TYPE", rafe.GetTaskType().ToString());
					tEvent.AddEventInfo("ATTEMPT_ID", rafe.GetAttemptId() == null ? string.Empty : rafe
						.GetAttemptId().ToString());
					tEvent.AddEventInfo("FINISH_TIME", rafe.GetFinishTime());
					tEvent.AddEventInfo("STATUS", rafe.GetTaskStatus());
					tEvent.AddEventInfo("STATE", rafe.GetState());
					tEvent.AddEventInfo("SHUFFLE_FINISH_TIME", rafe.GetShuffleFinishTime());
					tEvent.AddEventInfo("SORT_FINISH_TIME", rafe.GetSortFinishTime());
					tEvent.AddEventInfo("COUNTERS_GROUPS", CountersToJSON(rafe.GetCounters()));
					tEvent.AddEventInfo("HOSTNAME", rafe.GetHostname());
					tEvent.AddEventInfo("PORT", rafe.GetPort());
					tEvent.AddEventInfo("RACK_NAME", rafe.GetRackName());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(rafe.GetTaskId().ToString());
					tEntity.SetEntityType(MapreduceTaskEntityType);
					tEntity.AddRelatedEntity(MapreduceJobEntityType, jobId.ToString());
					break;
				}

				case EventType.SetupAttemptFinished:
				case EventType.CleanupAttemptFinished:
				{
					TaskAttemptFinishedEvent tafe = (TaskAttemptFinishedEvent)@event;
					tEvent.AddEventInfo("TASK_TYPE", tafe.GetTaskType().ToString());
					tEvent.AddEventInfo("ATTEMPT_ID", tafe.GetAttemptId() == null ? string.Empty : tafe
						.GetAttemptId().ToString());
					tEvent.AddEventInfo("FINISH_TIME", tafe.GetFinishTime());
					tEvent.AddEventInfo("STATUS", tafe.GetTaskStatus());
					tEvent.AddEventInfo("STATE", tafe.GetState());
					tEvent.AddEventInfo("COUNTERS_GROUPS", CountersToJSON(tafe.GetCounters()));
					tEvent.AddEventInfo("HOSTNAME", tafe.GetHostname());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(tafe.GetTaskId().ToString());
					tEntity.SetEntityType(MapreduceTaskEntityType);
					tEntity.AddRelatedEntity(MapreduceJobEntityType, jobId.ToString());
					break;
				}

				case EventType.AmStarted:
				{
					AMStartedEvent ase = (AMStartedEvent)@event;
					tEvent.AddEventInfo("APPLICATION_ATTEMPT_ID", ase.GetAppAttemptId() == null ? string.Empty
						 : ase.GetAppAttemptId().ToString());
					tEvent.AddEventInfo("CONTAINER_ID", ase.GetContainerId() == null ? string.Empty : 
						ase.GetContainerId().ToString());
					tEvent.AddEventInfo("NODE_MANAGER_HOST", ase.GetNodeManagerHost());
					tEvent.AddEventInfo("NODE_MANAGER_PORT", ase.GetNodeManagerPort());
					tEvent.AddEventInfo("NODE_MANAGER_HTTP_PORT", ase.GetNodeManagerHttpPort());
					tEvent.AddEventInfo("START_TIME", ase.GetStartTime());
					tEvent.AddEventInfo("SUBMIT_TIME", ase.GetSubmitTime());
					tEntity.AddEvent(tEvent);
					tEntity.SetEntityId(jobId.ToString());
					tEntity.SetEntityType(MapreduceJobEntityType);
					break;
				}

				default:
				{
					break;
				}
			}
			try
			{
				timelineClient.PutEntities(tEntity);
			}
			catch (IOException ex)
			{
				Log.Error("Error putting entity " + tEntity.GetEntityId() + " to Timeline" + "Server"
					, ex);
			}
			catch (YarnException ex)
			{
				Log.Error("Error putting entity " + tEntity.GetEntityId() + " to Timeline" + "Server"
					, ex);
			}
		}

		[InterfaceAudience.Private]
		public virtual JsonNode CountersToJSON(Counters counters)
		{
			ObjectMapper mapper = new ObjectMapper();
			ArrayNode nodes = ((ArrayNode)mapper.CreateArrayNode());
			if (counters != null)
			{
				foreach (CounterGroup counterGroup in counters)
				{
					ObjectNode groupNode = nodes.AddObject();
					groupNode.Put("NAME", counterGroup.GetName());
					groupNode.Put("DISPLAY_NAME", counterGroup.GetDisplayName());
					ArrayNode countersNode = groupNode.PutArray("COUNTERS");
					foreach (Counter counter in counterGroup)
					{
						ObjectNode counterNode = countersNode.AddObject();
						counterNode.Put("NAME", counter.GetName());
						counterNode.Put("DISPLAY_NAME", counter.GetDisplayName());
						counterNode.Put("VALUE", counter.GetValue());
					}
				}
			}
			return nodes;
		}

		private void SetSummarySlotSeconds(JobSummary summary, Counters allCounters)
		{
			Counter slotMillisMapCounter = allCounters.FindCounter(JobCounter.SlotsMillisMaps
				);
			if (slotMillisMapCounter != null)
			{
				summary.SetMapSlotSeconds(slotMillisMapCounter.GetValue() / 1000);
			}
			Counter slotMillisReduceCounter = allCounters.FindCounter(JobCounter.SlotsMillisReduces
				);
			if (slotMillisReduceCounter != null)
			{
				summary.SetReduceSlotSeconds(slotMillisReduceCounter.GetValue() / 1000);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CloseEventWriter(JobId jobId)
		{
			JobHistoryEventHandler.MetaInfo mi = fileMap[jobId];
			if (mi == null)
			{
				throw new IOException("No MetaInfo found for JobId: [" + jobId + "]");
			}
			if (!mi.IsWriterActive())
			{
				throw new IOException("Inactive Writer: Likely received multiple JobFinished / " 
					+ "JobUnsuccessful events for JobId: [" + jobId + "]");
			}
			// Close the Writer
			try
			{
				mi.CloseWriter();
			}
			catch (IOException e)
			{
				Log.Error("Error closing writer for JobID: " + jobId);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void ProcessDoneFiles(JobId jobId)
		{
			JobHistoryEventHandler.MetaInfo mi = fileMap[jobId];
			if (mi == null)
			{
				throw new IOException("No MetaInfo found for JobId: [" + jobId + "]");
			}
			if (mi.GetHistoryFile() == null)
			{
				Log.Warn("No file for job-history with " + jobId + " found in cache!");
			}
			if (mi.GetConfFile() == null)
			{
				Log.Warn("No file for jobconf with " + jobId + " found in cache!");
			}
			// Writing out the summary file.
			// TODO JH enhancement - reuse this file to store additional indexing info
			// like ACLs, etc. JHServer can use HDFS append to build an index file
			// with more info than is available via the filename.
			Path qualifiedSummaryDoneFile = null;
			FSDataOutputStream summaryFileOut = null;
			try
			{
				string doneSummaryFileName = GetTempFileName(JobHistoryUtils.GetIntermediateSummaryFileName
					(jobId));
				qualifiedSummaryDoneFile = doneDirFS.MakeQualified(new Path(doneDirPrefixPath, doneSummaryFileName
					));
				summaryFileOut = doneDirFS.Create(qualifiedSummaryDoneFile, true);
				summaryFileOut.WriteUTF(mi.GetJobSummary().GetJobSummaryString());
				summaryFileOut.Close();
				doneDirFS.SetPermission(qualifiedSummaryDoneFile, new FsPermission(JobHistoryUtils
					.HistoryIntermediateFilePermissions));
			}
			catch (IOException e)
			{
				Log.Info("Unable to write out JobSummaryInfo to [" + qualifiedSummaryDoneFile + "]"
					, e);
				throw;
			}
			try
			{
				// Move historyFile to Done Folder.
				Path qualifiedDoneFile = null;
				if (mi.GetHistoryFile() != null)
				{
					Path historyFile = mi.GetHistoryFile();
					Path qualifiedLogFile = stagingDirFS.MakeQualified(historyFile);
					string doneJobHistoryFileName = GetTempFileName(FileNameIndexUtils.GetDoneFileName
						(mi.GetJobIndexInfo()));
					qualifiedDoneFile = doneDirFS.MakeQualified(new Path(doneDirPrefixPath, doneJobHistoryFileName
						));
					MoveToDoneNow(qualifiedLogFile, qualifiedDoneFile);
				}
				// Move confFile to Done Folder
				Path qualifiedConfDoneFile = null;
				if (mi.GetConfFile() != null)
				{
					Path confFile = mi.GetConfFile();
					Path qualifiedConfFile = stagingDirFS.MakeQualified(confFile);
					string doneConfFileName = GetTempFileName(JobHistoryUtils.GetIntermediateConfFileName
						(jobId));
					qualifiedConfDoneFile = doneDirFS.MakeQualified(new Path(doneDirPrefixPath, doneConfFileName
						));
					MoveToDoneNow(qualifiedConfFile, qualifiedConfDoneFile);
				}
				MoveTmpToDone(qualifiedSummaryDoneFile);
				MoveTmpToDone(qualifiedConfDoneFile);
				MoveTmpToDone(qualifiedDoneFile);
			}
			catch (IOException e)
			{
				Log.Error("Error closing writer for JobID: " + jobId);
				throw;
			}
		}

		private class FlushTimerTask : TimerTask
		{
			private JobHistoryEventHandler.MetaInfo metaInfo;

			private IOException ioe = null;

			private volatile bool shouldRun = true;

			internal FlushTimerTask(JobHistoryEventHandler _enclosing, JobHistoryEventHandler.MetaInfo
				 metaInfo)
			{
				this._enclosing = _enclosing;
				this.metaInfo = metaInfo;
			}

			public override void Run()
			{
				JobHistoryEventHandler.Log.Debug("In flush timer task");
				lock (this._enclosing.Lock)
				{
					try
					{
						if (!this.metaInfo.IsTimerShutDown() && this.shouldRun)
						{
							this.metaInfo.Flush();
						}
					}
					catch (IOException e)
					{
						this.ioe = e;
					}
				}
			}

			public virtual IOException GetException()
			{
				return this.ioe;
			}

			public virtual void Stop()
			{
				this.shouldRun = false;
				this.Cancel();
			}

			private readonly JobHistoryEventHandler _enclosing;
		}

		protected internal class MetaInfo
		{
			private Path historyFile;

			private Path confFile;

			private EventWriter writer;

			internal JobIndexInfo jobIndexInfo;

			internal JobSummary jobSummary;

			internal Timer flushTimer;

			internal JobHistoryEventHandler.FlushTimerTask flushTimerTask;

			private bool isTimerShutDown = false;

			private string forcedJobStateOnShutDown;

			internal MetaInfo(JobHistoryEventHandler _enclosing, Path historyFile, Path conf, 
				EventWriter writer, string user, string jobName, JobId jobId, string forcedJobStateOnShutDown
				, string queueName)
			{
				this._enclosing = _enclosing;
				this.historyFile = historyFile;
				this.confFile = conf;
				this.writer = writer;
				this.jobIndexInfo = new JobIndexInfo(-1, -1, user, jobName, jobId, -1, -1, null, 
					queueName);
				this.jobSummary = new JobSummary();
				this.flushTimer = new Timer("FlushTimer", true);
				this.forcedJobStateOnShutDown = forcedJobStateOnShutDown;
			}

			internal virtual Path GetHistoryFile()
			{
				return this.historyFile;
			}

			internal virtual Path GetConfFile()
			{
				return this.confFile;
			}

			internal virtual JobIndexInfo GetJobIndexInfo()
			{
				return this.jobIndexInfo;
			}

			internal virtual JobSummary GetJobSummary()
			{
				return this.jobSummary;
			}

			internal virtual bool IsWriterActive()
			{
				return this.writer != null;
			}

			internal virtual bool IsTimerShutDown()
			{
				return this.isTimerShutDown;
			}

			internal virtual string GetForcedJobStateOnShutDown()
			{
				return this.forcedJobStateOnShutDown;
			}

			public override string ToString()
			{
				return "Job MetaInfo for " + this.jobSummary.GetJobId() + " history file " + this
					.historyFile;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void CloseWriter()
			{
				JobHistoryEventHandler.Log.Debug("Closing Writer");
				lock (this._enclosing.Lock)
				{
					if (this.writer != null)
					{
						this.writer.Close();
					}
					this.writer = null;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void WriteEvent(HistoryEvent @event)
			{
				JobHistoryEventHandler.Log.Debug("Writing event");
				lock (this._enclosing.Lock)
				{
					if (this.writer != null)
					{
						this.writer.Write(@event);
						this.ProcessEventForFlush(@event);
						this.MaybeFlush(@event);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void ProcessEventForFlush(HistoryEvent historyEvent)
			{
				if (EnumSet.Of(EventType.MapAttemptFinished, EventType.MapAttemptFailed, EventType
					.MapAttemptKilled, EventType.ReduceAttemptFinished, EventType.ReduceAttemptFailed
					, EventType.ReduceAttemptKilled, EventType.TaskFinished, EventType.TaskFailed, EventType
					.JobFinished, EventType.JobFailed, EventType.JobKilled).Contains(historyEvent.GetEventType
					()))
				{
					this._enclosing.numUnflushedCompletionEvents++;
					if (!this._enclosing.isTimerActive)
					{
						this.ResetFlushTimer();
						if (!this.isTimerShutDown)
						{
							this.flushTimerTask = new JobHistoryEventHandler.FlushTimerTask(this, this);
							this.flushTimer.Schedule(this.flushTimerTask, this._enclosing.flushTimeout);
							this._enclosing.isTimerActive = true;
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void ResetFlushTimer()
			{
				if (this.flushTimerTask != null)
				{
					IOException exception = this.flushTimerTask.GetException();
					this.flushTimerTask.Stop();
					if (exception != null)
					{
						throw exception;
					}
					this.flushTimerTask = null;
				}
				this._enclosing.isTimerActive = false;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void MaybeFlush(HistoryEvent historyEvent)
			{
				if ((this._enclosing.eventQueue.Count < this._enclosing.minQueueSizeForBatchingFlushes
					 && this._enclosing.numUnflushedCompletionEvents > 0) || this._enclosing.numUnflushedCompletionEvents
					 >= this._enclosing.maxUnflushedCompletionEvents || this._enclosing.IsJobCompletionEvent
					(historyEvent))
				{
					this.Flush();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void Flush()
			{
				if (JobHistoryEventHandler.Log.IsDebugEnabled())
				{
					JobHistoryEventHandler.Log.Debug("Flushing " + this.ToString());
				}
				lock (this._enclosing.Lock)
				{
					if (this._enclosing.numUnflushedCompletionEvents != 0)
					{
						// skipped timer cancel.
						this.writer.Flush();
						this._enclosing.numUnflushedCompletionEvents = 0;
						this.ResetFlushTimer();
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void ShutDownTimer()
			{
				if (JobHistoryEventHandler.Log.IsDebugEnabled())
				{
					JobHistoryEventHandler.Log.Debug("Shutting down timer " + this.ToString());
				}
				lock (this._enclosing.Lock)
				{
					this.isTimerShutDown = true;
					this.flushTimer.Cancel();
					if (this.flushTimerTask != null && this.flushTimerTask.GetException() != null)
					{
						throw this.flushTimerTask.GetException();
					}
				}
			}

			private readonly JobHistoryEventHandler _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		private void MoveTmpToDone(Path tmpPath)
		{
			if (tmpPath != null)
			{
				string tmpFileName = tmpPath.GetName();
				string fileName = GetFileNameFromTmpFN(tmpFileName);
				Path path = new Path(tmpPath.GetParent(), fileName);
				doneDirFS.Rename(tmpPath, path);
				Log.Info("Moved tmp to done: " + tmpPath + " to " + path);
			}
		}

		// TODO If the FS objects are the same, this should be a rename instead of a
		// copy.
		/// <exception cref="System.IO.IOException"/>
		private void MoveToDoneNow(Path fromPath, Path toPath)
		{
			// check if path exists, in case of retries it may not exist
			if (stagingDirFS.Exists(fromPath))
			{
				Log.Info("Copying " + fromPath.ToString() + " to " + toPath.ToString());
				// TODO temporarily removing the existing dst
				if (doneDirFS.Exists(toPath))
				{
					doneDirFS.Delete(toPath, true);
				}
				bool copied = FileUtil.Copy(stagingDirFS, fromPath, doneDirFS, toPath, false, GetConfig
					());
				if (copied)
				{
					Log.Info("Copied to done location: " + toPath);
				}
				else
				{
					Log.Info("copy failed");
				}
				doneDirFS.SetPermission(toPath, new FsPermission(JobHistoryUtils.HistoryIntermediateFilePermissions
					));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool PathExists(FileSystem fileSys, Path path)
		{
			return fileSys.Exists(path);
		}

		private string GetTempFileName(string srcFile)
		{
			return srcFile + "_tmp";
		}

		private string GetFileNameFromTmpFN(string tmpFileName)
		{
			//TODO. Some error checking here.
			return Sharpen.Runtime.Substring(tmpFileName, 0, tmpFileName.Length - 4);
		}

		public virtual void SetForcejobCompletion(bool forceJobCompletion)
		{
			this.forceJobCompletion = forceJobCompletion;
			Log.Info("JobHistoryEventHandler notified that forceJobCompletion is " + forceJobCompletion
				);
		}

		private string CreateJobStateForJobUnsuccessfulCompletionEvent(string forcedJobStateOnShutDown
			)
		{
			if (forcedJobStateOnShutDown == null || forcedJobStateOnShutDown.IsEmpty())
			{
				return JobState.Killed.ToString();
			}
			else
			{
				if (forcedJobStateOnShutDown.Equals(JobStateInternal.Error.ToString()) || forcedJobStateOnShutDown
					.Equals(JobStateInternal.Failed.ToString()))
				{
					return JobState.Failed.ToString();
				}
				else
				{
					if (forcedJobStateOnShutDown.Equals(JobStateInternal.Succeeded.ToString()))
					{
						return JobState.Succeeded.ToString();
					}
				}
			}
			return JobState.Killed.ToString();
		}

		[VisibleForTesting]
		internal virtual bool GetFlushTimerStatus()
		{
			return isTimerActive;
		}
	}
}
