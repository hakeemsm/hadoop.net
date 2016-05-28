using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TypeConverter
	{
		private static RecordFactory recordFactory;

		static TypeConverter()
		{
			recordFactory = RecordFactoryProvider.GetRecordFactory(null);
		}

		public static JobID FromYarn(JobId id)
		{
			string identifier = FromClusterTimeStamp(id.GetAppId().GetClusterTimestamp());
			return new JobID(identifier, id.GetId());
		}

		//currently there is 1-1 mapping between appid and jobid
		public static JobID FromYarn(ApplicationId appID)
		{
			string identifier = FromClusterTimeStamp(appID.GetClusterTimestamp());
			return new JobID(identifier, appID.GetId());
		}

		public static JobId ToYarn(JobID id)
		{
			JobId jobId = recordFactory.NewRecordInstance<JobId>();
			jobId.SetId(id.GetId());
			//currently there is 1-1 mapping between appid and jobid
			ApplicationId appId = ApplicationId.NewInstance(ToClusterTimeStamp(id.GetJtIdentifier
				()), id.GetId());
			jobId.SetAppId(appId);
			return jobId;
		}

		private static string FromClusterTimeStamp(long clusterTimeStamp)
		{
			return System.Convert.ToString(clusterTimeStamp);
		}

		private static long ToClusterTimeStamp(string identifier)
		{
			return long.Parse(identifier);
		}

		public static TaskType FromYarn(TaskType taskType)
		{
			switch (taskType)
			{
				case TaskType.Map:
				{
					return TaskType.Map;
				}

				case TaskType.Reduce:
				{
					return TaskType.Reduce;
				}

				default:
				{
					throw new YarnRuntimeException("Unrecognized task type: " + taskType);
				}
			}
		}

		public static TaskType ToYarn(TaskType taskType)
		{
			switch (taskType)
			{
				case TaskType.Map:
				{
					return TaskType.Map;
				}

				case TaskType.Reduce:
				{
					return TaskType.Reduce;
				}

				default:
				{
					throw new YarnRuntimeException("Unrecognized task type: " + taskType);
				}
			}
		}

		public static TaskID FromYarn(TaskId id)
		{
			return new TaskID(FromYarn(id.GetJobId()), FromYarn(id.GetTaskType()), id.GetId()
				);
		}

		public static TaskId ToYarn(TaskID id)
		{
			TaskId taskId = recordFactory.NewRecordInstance<TaskId>();
			taskId.SetId(id.GetId());
			taskId.SetTaskType(ToYarn(id.GetTaskType()));
			taskId.SetJobId(ToYarn(id.GetJobID()));
			return taskId;
		}

		public static TaskAttemptState ToYarn(TaskStatus.State state)
		{
			switch (state)
			{
				case TaskStatus.State.CommitPending:
				{
					return TaskAttemptState.CommitPending;
				}

				case TaskStatus.State.Failed:
				case TaskStatus.State.FailedUnclean:
				{
					return TaskAttemptState.Failed;
				}

				case TaskStatus.State.Killed:
				case TaskStatus.State.KilledUnclean:
				{
					return TaskAttemptState.Killed;
				}

				case TaskStatus.State.Running:
				{
					return TaskAttemptState.Running;
				}

				case TaskStatus.State.Succeeded:
				{
					return TaskAttemptState.Succeeded;
				}

				case TaskStatus.State.Unassigned:
				{
					return TaskAttemptState.Starting;
				}

				default:
				{
					throw new YarnRuntimeException("Unrecognized State: " + state);
				}
			}
		}

		public static Phase ToYarn(TaskStatus.Phase phase)
		{
			switch (phase)
			{
				case TaskStatus.Phase.Starting:
				{
					return Phase.Starting;
				}

				case TaskStatus.Phase.Map:
				{
					return Phase.Map;
				}

				case TaskStatus.Phase.Shuffle:
				{
					return Phase.Shuffle;
				}

				case TaskStatus.Phase.Sort:
				{
					return Phase.Sort;
				}

				case TaskStatus.Phase.Reduce:
				{
					return Phase.Reduce;
				}

				case TaskStatus.Phase.Cleanup:
				{
					return Phase.Cleanup;
				}
			}
			throw new YarnRuntimeException("Unrecognized Phase: " + phase);
		}

		public static TaskCompletionEvent[] FromYarn(TaskAttemptCompletionEvent[] newEvents
			)
		{
			TaskCompletionEvent[] oldEvents = new TaskCompletionEvent[newEvents.Length];
			int i = 0;
			foreach (TaskAttemptCompletionEvent newEvent in newEvents)
			{
				oldEvents[i++] = FromYarn(newEvent);
			}
			return oldEvents;
		}

		public static TaskCompletionEvent FromYarn(TaskAttemptCompletionEvent newEvent)
		{
			return new TaskCompletionEvent(newEvent.GetEventId(), FromYarn(newEvent.GetAttemptId
				()), newEvent.GetAttemptId().GetId(), newEvent.GetAttemptId().GetTaskId().GetTaskType
				().Equals(TaskType.Map), FromYarn(newEvent.GetStatus()), newEvent.GetMapOutputServerAddress
				());
		}

		public static TaskCompletionEvent.Status FromYarn(TaskAttemptCompletionEventStatus
			 newStatus)
		{
			switch (newStatus)
			{
				case TaskAttemptCompletionEventStatus.Failed:
				{
					return TaskCompletionEvent.Status.Failed;
				}

				case TaskAttemptCompletionEventStatus.Killed:
				{
					return TaskCompletionEvent.Status.Killed;
				}

				case TaskAttemptCompletionEventStatus.Obsolete:
				{
					return TaskCompletionEvent.Status.Obsolete;
				}

				case TaskAttemptCompletionEventStatus.Succeeded:
				{
					return TaskCompletionEvent.Status.Succeeded;
				}

				case TaskAttemptCompletionEventStatus.Tipfailed:
				{
					return TaskCompletionEvent.Status.Tipfailed;
				}
			}
			throw new YarnRuntimeException("Unrecognized status: " + newStatus);
		}

		public static TaskAttemptID FromYarn(TaskAttemptId id)
		{
			return new TaskAttemptID(FromYarn(id.GetTaskId()), id.GetId());
		}

		public static TaskAttemptId ToYarn(TaskAttemptID id)
		{
			TaskAttemptId taskAttemptId = recordFactory.NewRecordInstance<TaskAttemptId>();
			taskAttemptId.SetTaskId(ToYarn(((TaskID)id.GetTaskID())));
			taskAttemptId.SetId(id.GetId());
			return taskAttemptId;
		}

		public static TaskAttemptId ToYarn(TaskAttemptID id)
		{
			TaskAttemptId taskAttemptId = recordFactory.NewRecordInstance<TaskAttemptId>();
			taskAttemptId.SetTaskId(ToYarn(id.GetTaskID()));
			taskAttemptId.SetId(id.GetId());
			return taskAttemptId;
		}

		public static Counters FromYarn(Counters yCntrs)
		{
			if (yCntrs == null)
			{
				return null;
			}
			Counters counters = new Counters();
			foreach (CounterGroup yGrp in yCntrs.GetAllCounterGroups().Values)
			{
				counters.AddGroup(yGrp.GetName(), yGrp.GetDisplayName());
				foreach (Counter yCntr in yGrp.GetAllCounters().Values)
				{
					Counter c = counters.FindCounter(yGrp.GetName(), yCntr.GetName());
					// if c can be found, or it will be skipped.
					if (c != null)
					{
						c.SetValue(yCntr.GetValue());
					}
				}
			}
			return counters;
		}

		public static Counters ToYarn(Counters counters)
		{
			if (counters == null)
			{
				return null;
			}
			Counters yCntrs = recordFactory.NewRecordInstance<Counters>();
			yCntrs.AddAllCounterGroups(new Dictionary<string, CounterGroup>());
			foreach (Counters.Group grp in counters)
			{
				CounterGroup yGrp = recordFactory.NewRecordInstance<CounterGroup>();
				yGrp.SetName(grp.GetName());
				yGrp.SetDisplayName(grp.GetDisplayName());
				yGrp.AddAllCounters(new Dictionary<string, Counter>());
				foreach (Counters.Counter cntr in grp)
				{
					Counter yCntr = recordFactory.NewRecordInstance<Counter>();
					yCntr.SetName(cntr.GetName());
					yCntr.SetDisplayName(cntr.GetDisplayName());
					yCntr.SetValue(cntr.GetValue());
					yGrp.SetCounter(yCntr.GetName(), yCntr);
				}
				yCntrs.SetCounterGroup(yGrp.GetName(), yGrp);
			}
			return yCntrs;
		}

		public static Counters ToYarn(Counters counters)
		{
			if (counters == null)
			{
				return null;
			}
			Counters yCntrs = recordFactory.NewRecordInstance<Counters>();
			yCntrs.AddAllCounterGroups(new Dictionary<string, CounterGroup>());
			foreach (CounterGroup grp in counters)
			{
				CounterGroup yGrp = recordFactory.NewRecordInstance<CounterGroup>();
				yGrp.SetName(grp.GetName());
				yGrp.SetDisplayName(grp.GetDisplayName());
				yGrp.AddAllCounters(new Dictionary<string, Counter>());
				foreach (Counter cntr in grp)
				{
					Counter yCntr = recordFactory.NewRecordInstance<Counter>();
					yCntr.SetName(cntr.GetName());
					yCntr.SetDisplayName(cntr.GetDisplayName());
					yCntr.SetValue(cntr.GetValue());
					yGrp.SetCounter(yCntr.GetName(), yCntr);
				}
				yCntrs.SetCounterGroup(yGrp.GetName(), yGrp);
			}
			return yCntrs;
		}

		public static JobStatus FromYarn(JobReport jobreport, string trackingUrl)
		{
			JobPriority jobPriority = JobPriority.Normal;
			JobStatus jobStatus = new JobStatus(FromYarn(jobreport.GetJobId()), jobreport.GetSetupProgress
				(), jobreport.GetMapProgress(), jobreport.GetReduceProgress(), jobreport.GetCleanupProgress
				(), FromYarn(jobreport.GetJobState()), jobPriority, jobreport.GetUser(), jobreport
				.GetJobName(), jobreport.GetJobFile(), trackingUrl, jobreport.IsUber());
			jobStatus.SetStartTime(jobreport.GetStartTime());
			jobStatus.SetFinishTime(jobreport.GetFinishTime());
			jobStatus.SetFailureInfo(jobreport.GetDiagnostics());
			return jobStatus;
		}

		public static QueueState FromYarn(QueueState state)
		{
			QueueState qState = QueueState.GetState(StringUtils.ToLowerCase(state.ToString())
				);
			return qState;
		}

		public static int FromYarn(JobState state)
		{
			switch (state)
			{
				case JobState.New:
				case JobState.Inited:
				{
					return JobStatus.Prep;
				}

				case JobState.Running:
				{
					return JobStatus.Running;
				}

				case JobState.Killed:
				{
					return JobStatus.Killed;
				}

				case JobState.Succeeded:
				{
					return JobStatus.Succeeded;
				}

				case JobState.Failed:
				case JobState.Error:
				{
					return JobStatus.Failed;
				}
			}
			throw new YarnRuntimeException("Unrecognized job state: " + state);
		}

		public static TIPStatus FromYarn(TaskState state)
		{
			switch (state)
			{
				case TaskState.New:
				case TaskState.Scheduled:
				{
					return TIPStatus.Pending;
				}

				case TaskState.Running:
				{
					return TIPStatus.Running;
				}

				case TaskState.Killed:
				{
					return TIPStatus.Killed;
				}

				case TaskState.Succeeded:
				{
					return TIPStatus.Complete;
				}

				case TaskState.Failed:
				{
					return TIPStatus.Failed;
				}
			}
			throw new YarnRuntimeException("Unrecognized task state: " + state);
		}

		public static TaskReport FromYarn(TaskReport report)
		{
			string[] diagnostics = null;
			if (report.GetDiagnosticsList() != null)
			{
				diagnostics = new string[report.GetDiagnosticsCount()];
				int i = 0;
				foreach (string cs in report.GetDiagnosticsList())
				{
					diagnostics[i++] = cs.ToString();
				}
			}
			else
			{
				diagnostics = new string[0];
			}
			TaskReport rep = new TaskReport(FromYarn(report.GetTaskId()), report.GetProgress(
				), report.GetTaskState().ToString(), diagnostics, FromYarn(report.GetTaskState()
				), report.GetStartTime(), report.GetFinishTime(), FromYarn(report.GetCounters())
				);
			IList<TaskAttemptID> runningAtts = new AList<TaskAttemptID>();
			foreach (TaskAttemptId id in report.GetRunningAttemptsList())
			{
				runningAtts.AddItem(FromYarn(id));
			}
			rep.SetRunningTaskAttemptIds(runningAtts);
			if (report.GetSuccessfulAttempt() != null)
			{
				rep.SetSuccessfulAttemptId(FromYarn(report.GetSuccessfulAttempt()));
			}
			return rep;
		}

		public static IList<TaskReport> FromYarn(IList<TaskReport> taskReports)
		{
			IList<TaskReport> reports = new AList<TaskReport>();
			foreach (TaskReport r in taskReports)
			{
				reports.AddItem(FromYarn(r));
			}
			return reports;
		}

		public static JobStatus.State FromYarn(YarnApplicationState yarnApplicationState, 
			FinalApplicationStatus finalApplicationStatus)
		{
			switch (yarnApplicationState)
			{
				case YarnApplicationState.New:
				case YarnApplicationState.NewSaving:
				case YarnApplicationState.Submitted:
				case YarnApplicationState.Accepted:
				{
					return JobStatus.State.Prep;
				}

				case YarnApplicationState.Running:
				{
					return JobStatus.State.Running;
				}

				case YarnApplicationState.Finished:
				{
					if (finalApplicationStatus == FinalApplicationStatus.Succeeded)
					{
						return JobStatus.State.Succeeded;
					}
					else
					{
						if (finalApplicationStatus == FinalApplicationStatus.Killed)
						{
							return JobStatus.State.Killed;
						}
					}
					goto case YarnApplicationState.Failed;
				}

				case YarnApplicationState.Failed:
				{
					return JobStatus.State.Failed;
				}

				case YarnApplicationState.Killed:
				{
					return JobStatus.State.Killed;
				}
			}
			throw new YarnRuntimeException("Unrecognized application state: " + yarnApplicationState
				);
		}

		private const string TtNamePrefix = "tracker_";

		public static TaskTrackerInfo FromYarn(NodeReport node)
		{
			TaskTrackerInfo taskTracker = new TaskTrackerInfo(TtNamePrefix + node.GetNodeId()
				.ToString());
			return taskTracker;
		}

		public static TaskTrackerInfo[] FromYarnNodes(IList<NodeReport> nodes)
		{
			IList<TaskTrackerInfo> taskTrackers = new AList<TaskTrackerInfo>();
			foreach (NodeReport node in nodes)
			{
				taskTrackers.AddItem(FromYarn(node));
			}
			return Sharpen.Collections.ToArray(taskTrackers, new TaskTrackerInfo[nodes.Count]
				);
		}

		public static JobStatus FromYarn(ApplicationReport application, string jobFile)
		{
			string trackingUrl = application.GetTrackingUrl();
			trackingUrl = trackingUrl == null ? string.Empty : trackingUrl;
			JobStatus jobStatus = new JobStatus(TypeConverter.FromYarn(application.GetApplicationId
				()), 0.0f, 0.0f, 0.0f, 0.0f, TypeConverter.FromYarn(application.GetYarnApplicationState
				(), application.GetFinalApplicationStatus()), JobPriority.Normal, application.GetUser
				(), application.GetName(), application.GetQueue(), jobFile, trackingUrl, false);
			jobStatus.SetSchedulingInfo(trackingUrl);
			// Set AM tracking url
			jobStatus.SetStartTime(application.GetStartTime());
			jobStatus.SetFinishTime(application.GetFinishTime());
			jobStatus.SetFailureInfo(application.GetDiagnostics());
			ApplicationResourceUsageReport resourceUsageReport = application.GetApplicationResourceUsageReport
				();
			if (resourceUsageReport != null)
			{
				jobStatus.SetNeededMem(resourceUsageReport.GetNeededResources().GetMemory());
				jobStatus.SetNumReservedSlots(resourceUsageReport.GetNumReservedContainers());
				jobStatus.SetNumUsedSlots(resourceUsageReport.GetNumUsedContainers());
				jobStatus.SetReservedMem(resourceUsageReport.GetReservedResources().GetMemory());
				jobStatus.SetUsedMem(resourceUsageReport.GetUsedResources().GetMemory());
			}
			return jobStatus;
		}

		public static JobStatus[] FromYarnApps(IList<ApplicationReport> applications, Configuration
			 conf)
		{
			IList<JobStatus> jobStatuses = new AList<JobStatus>();
			foreach (ApplicationReport application in applications)
			{
				// each applicationReport has its own jobFile
				JobID jobId = TypeConverter.FromYarn(application.GetApplicationId());
				jobStatuses.AddItem(TypeConverter.FromYarn(application, MRApps.GetJobFile(conf, application
					.GetUser(), jobId)));
			}
			return Sharpen.Collections.ToArray(jobStatuses, new JobStatus[jobStatuses.Count]);
		}

		public static QueueInfo FromYarn(QueueInfo queueInfo, Configuration conf)
		{
			QueueInfo toReturn = new QueueInfo(queueInfo.GetQueueName(), "Capacity: " + queueInfo
				.GetCapacity() * 100 + ", MaximumCapacity: " + (queueInfo.GetMaximumCapacity() <
				 0 ? "UNDEFINED" : queueInfo.GetMaximumCapacity() * 100) + ", CurrentCapacity: "
				 + queueInfo.GetCurrentCapacity() * 100, FromYarn(queueInfo.GetQueueState()), TypeConverter
				.FromYarnApps(queueInfo.GetApplications(), conf));
			IList<QueueInfo> childQueues = new AList<QueueInfo>();
			foreach (QueueInfo childQueue in queueInfo.GetChildQueues())
			{
				childQueues.AddItem(FromYarn(childQueue, conf));
			}
			toReturn.SetQueueChildren(childQueues);
			return toReturn;
		}

		public static QueueInfo[] FromYarnQueueInfo(IList<QueueInfo> queues, Configuration
			 conf)
		{
			IList<QueueInfo> queueInfos = new AList<QueueInfo>(queues.Count);
			foreach (QueueInfo queue in queues)
			{
				queueInfos.AddItem(TypeConverter.FromYarn(queue, conf));
			}
			return Sharpen.Collections.ToArray(queueInfos, new QueueInfo[queueInfos.Count]);
		}

		public static QueueAclsInfo[] FromYarnQueueUserAclsInfo(IList<QueueUserACLInfo> userAcls
			)
		{
			IList<QueueAclsInfo> acls = new AList<QueueAclsInfo>();
			foreach (QueueUserACLInfo aclInfo in userAcls)
			{
				IList<string> operations = new AList<string>();
				foreach (QueueACL qAcl in aclInfo.GetUserAcls())
				{
					operations.AddItem(qAcl.ToString());
				}
				QueueAclsInfo acl = new QueueAclsInfo(aclInfo.GetQueueName(), Sharpen.Collections.ToArray
					(operations, new string[operations.Count]));
				acls.AddItem(acl);
			}
			return Sharpen.Collections.ToArray(acls, new QueueAclsInfo[acls.Count]);
		}
	}
}
