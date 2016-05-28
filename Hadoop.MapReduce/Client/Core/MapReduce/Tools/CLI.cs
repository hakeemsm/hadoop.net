using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Tools
{
	/// <summary>Interprets the map reduce cli options</summary>
	public class CLI : Configured, Tool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(CLI));

		protected internal Cluster cluster;

		private static readonly ICollection<string> taskTypes = new HashSet<string>(Arrays
			.AsList("MAP", "REDUCE"));

		private readonly ICollection<string> taskStates = new HashSet<string>(Arrays.AsList
			("running", "completed", "pending", "failed", "killed"));

		public CLI()
		{
		}

		public CLI(Configuration conf)
		{
			SetConf(conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] argv)
		{
			int exitCode = -1;
			if (argv.Length < 1)
			{
				DisplayUsage(string.Empty);
				return exitCode;
			}
			// process arguments
			string cmd = argv[0];
			string submitJobFile = null;
			string jobid = null;
			string taskid = null;
			string historyFile = null;
			string counterGroupName = null;
			string counterName = null;
			JobPriority jp = null;
			string taskType = null;
			string taskState = null;
			int fromEvent = 0;
			int nEvents = 0;
			bool getStatus = false;
			bool getCounter = false;
			bool killJob = false;
			bool listEvents = false;
			bool viewHistory = false;
			bool viewAllHistory = false;
			bool listJobs = false;
			bool listAllJobs = false;
			bool listActiveTrackers = false;
			bool listBlacklistedTrackers = false;
			bool displayTasks = false;
			bool killTask = false;
			bool failTask = false;
			bool setJobPriority = false;
			bool logs = false;
			if ("-submit".Equals(cmd))
			{
				if (argv.Length != 2)
				{
					DisplayUsage(cmd);
					return exitCode;
				}
				submitJobFile = argv[1];
			}
			else
			{
				if ("-status".Equals(cmd))
				{
					if (argv.Length != 2)
					{
						DisplayUsage(cmd);
						return exitCode;
					}
					jobid = argv[1];
					getStatus = true;
				}
				else
				{
					if ("-counter".Equals(cmd))
					{
						if (argv.Length != 4)
						{
							DisplayUsage(cmd);
							return exitCode;
						}
						getCounter = true;
						jobid = argv[1];
						counterGroupName = argv[2];
						counterName = argv[3];
					}
					else
					{
						if ("-kill".Equals(cmd))
						{
							if (argv.Length != 2)
							{
								DisplayUsage(cmd);
								return exitCode;
							}
							jobid = argv[1];
							killJob = true;
						}
						else
						{
							if ("-set-priority".Equals(cmd))
							{
								if (argv.Length != 3)
								{
									DisplayUsage(cmd);
									return exitCode;
								}
								jobid = argv[1];
								try
								{
									jp = JobPriority.ValueOf(argv[2]);
								}
								catch (ArgumentException iae)
								{
									Log.Info(iae);
									DisplayUsage(cmd);
									return exitCode;
								}
								setJobPriority = true;
							}
							else
							{
								if ("-events".Equals(cmd))
								{
									if (argv.Length != 4)
									{
										DisplayUsage(cmd);
										return exitCode;
									}
									jobid = argv[1];
									fromEvent = System.Convert.ToInt32(argv[2]);
									nEvents = System.Convert.ToInt32(argv[3]);
									listEvents = true;
								}
								else
								{
									if ("-history".Equals(cmd))
									{
										if (argv.Length != 2 && !(argv.Length == 3 && "all".Equals(argv[1])))
										{
											DisplayUsage(cmd);
											return exitCode;
										}
										viewHistory = true;
										if (argv.Length == 3 && "all".Equals(argv[1]))
										{
											viewAllHistory = true;
											historyFile = argv[2];
										}
										else
										{
											historyFile = argv[1];
										}
									}
									else
									{
										if ("-list".Equals(cmd))
										{
											if (argv.Length != 1 && !(argv.Length == 2 && "all".Equals(argv[1])))
											{
												DisplayUsage(cmd);
												return exitCode;
											}
											if (argv.Length == 2 && "all".Equals(argv[1]))
											{
												listAllJobs = true;
											}
											else
											{
												listJobs = true;
											}
										}
										else
										{
											if ("-kill-task".Equals(cmd))
											{
												if (argv.Length != 2)
												{
													DisplayUsage(cmd);
													return exitCode;
												}
												killTask = true;
												taskid = argv[1];
											}
											else
											{
												if ("-fail-task".Equals(cmd))
												{
													if (argv.Length != 2)
													{
														DisplayUsage(cmd);
														return exitCode;
													}
													failTask = true;
													taskid = argv[1];
												}
												else
												{
													if ("-list-active-trackers".Equals(cmd))
													{
														if (argv.Length != 1)
														{
															DisplayUsage(cmd);
															return exitCode;
														}
														listActiveTrackers = true;
													}
													else
													{
														if ("-list-blacklisted-trackers".Equals(cmd))
														{
															if (argv.Length != 1)
															{
																DisplayUsage(cmd);
																return exitCode;
															}
															listBlacklistedTrackers = true;
														}
														else
														{
															if ("-list-attempt-ids".Equals(cmd))
															{
																if (argv.Length != 4)
																{
																	DisplayUsage(cmd);
																	return exitCode;
																}
																jobid = argv[1];
																taskType = argv[2];
																taskState = argv[3];
																displayTasks = true;
																if (!taskTypes.Contains(StringUtils.ToUpperCase(taskType)))
																{
																	System.Console.Out.WriteLine("Error: Invalid task-type: " + taskType);
																	DisplayUsage(cmd);
																	return exitCode;
																}
																if (!taskStates.Contains(StringUtils.ToLowerCase(taskState)))
																{
																	System.Console.Out.WriteLine("Error: Invalid task-state: " + taskState);
																	DisplayUsage(cmd);
																	return exitCode;
																}
															}
															else
															{
																if ("-logs".Equals(cmd))
																{
																	if (argv.Length == 2 || argv.Length == 3)
																	{
																		logs = true;
																		jobid = argv[1];
																		if (argv.Length == 3)
																		{
																			taskid = argv[2];
																		}
																		else
																		{
																			taskid = null;
																		}
																	}
																	else
																	{
																		DisplayUsage(cmd);
																		return exitCode;
																	}
																}
																else
																{
																	DisplayUsage(cmd);
																	return exitCode;
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			// initialize cluster
			cluster = CreateCluster();
			// Submit the request
			try
			{
				if (submitJobFile != null)
				{
					Job job = Job.GetInstance(new JobConf(submitJobFile));
					job.Submit();
					System.Console.Out.WriteLine("Created job " + job.GetJobID());
					exitCode = 0;
				}
				else
				{
					if (getStatus)
					{
						Job job = cluster.GetJob(JobID.ForName(jobid));
						if (job == null)
						{
							System.Console.Out.WriteLine("Could not find job " + jobid);
						}
						else
						{
							Counters counters = job.GetCounters();
							System.Console.Out.WriteLine();
							System.Console.Out.WriteLine(job);
							if (counters != null)
							{
								System.Console.Out.WriteLine(counters);
							}
							else
							{
								System.Console.Out.WriteLine("Counters not available. Job is retired.");
							}
							exitCode = 0;
						}
					}
					else
					{
						if (getCounter)
						{
							Job job = cluster.GetJob(JobID.ForName(jobid));
							if (job == null)
							{
								System.Console.Out.WriteLine("Could not find job " + jobid);
							}
							else
							{
								Counters counters = job.GetCounters();
								if (counters == null)
								{
									System.Console.Out.WriteLine("Counters not available for retired job " + jobid);
									exitCode = -1;
								}
								else
								{
									System.Console.Out.WriteLine(GetCounter(counters, counterGroupName, counterName));
									exitCode = 0;
								}
							}
						}
						else
						{
							if (killJob)
							{
								Job job = cluster.GetJob(JobID.ForName(jobid));
								if (job == null)
								{
									System.Console.Out.WriteLine("Could not find job " + jobid);
								}
								else
								{
									JobStatus jobStatus = job.GetStatus();
									if (jobStatus.GetState() == JobStatus.State.Failed)
									{
										System.Console.Out.WriteLine("Could not mark the job " + jobid + " as killed, as it has already failed."
											);
										exitCode = -1;
									}
									else
									{
										if (jobStatus.GetState() == JobStatus.State.Killed)
										{
											System.Console.Out.WriteLine("The job " + jobid + " has already been killed.");
											exitCode = -1;
										}
										else
										{
											if (jobStatus.GetState() == JobStatus.State.Succeeded)
											{
												System.Console.Out.WriteLine("Could not kill the job " + jobid + ", as it has already succeeded."
													);
												exitCode = -1;
											}
											else
											{
												job.KillJob();
												System.Console.Out.WriteLine("Killed job " + jobid);
												exitCode = 0;
											}
										}
									}
								}
							}
							else
							{
								if (setJobPriority)
								{
									Job job = cluster.GetJob(JobID.ForName(jobid));
									if (job == null)
									{
										System.Console.Out.WriteLine("Could not find job " + jobid);
									}
									else
									{
										job.SetPriority(jp);
										System.Console.Out.WriteLine("Changed job priority.");
										exitCode = 0;
									}
								}
								else
								{
									if (viewHistory)
									{
										ViewHistory(historyFile, viewAllHistory);
										exitCode = 0;
									}
									else
									{
										if (listEvents)
										{
											ListEvents(cluster.GetJob(JobID.ForName(jobid)), fromEvent, nEvents);
											exitCode = 0;
										}
										else
										{
											if (listJobs)
											{
												ListJobs(cluster);
												exitCode = 0;
											}
											else
											{
												if (listAllJobs)
												{
													ListAllJobs(cluster);
													exitCode = 0;
												}
												else
												{
													if (listActiveTrackers)
													{
														ListActiveTrackers(cluster);
														exitCode = 0;
													}
													else
													{
														if (listBlacklistedTrackers)
														{
															ListBlacklistedTrackers(cluster);
															exitCode = 0;
														}
														else
														{
															if (displayTasks)
															{
																DisplayTasks(cluster.GetJob(JobID.ForName(jobid)), taskType, taskState);
																exitCode = 0;
															}
															else
															{
																if (killTask)
																{
																	TaskAttemptID taskID = TaskAttemptID.ForName(taskid);
																	Job job = cluster.GetJob(taskID.GetJobID());
																	if (job == null)
																	{
																		System.Console.Out.WriteLine("Could not find job " + jobid);
																	}
																	else
																	{
																		if (job.KillTask(taskID, false))
																		{
																			System.Console.Out.WriteLine("Killed task " + taskid);
																			exitCode = 0;
																		}
																		else
																		{
																			System.Console.Out.WriteLine("Could not kill task " + taskid);
																			exitCode = -1;
																		}
																	}
																}
																else
																{
																	if (failTask)
																	{
																		TaskAttemptID taskID = TaskAttemptID.ForName(taskid);
																		Job job = cluster.GetJob(taskID.GetJobID());
																		if (job == null)
																		{
																			System.Console.Out.WriteLine("Could not find job " + jobid);
																		}
																		else
																		{
																			if (job.KillTask(taskID, true))
																			{
																				System.Console.Out.WriteLine("Killed task " + taskID + " by failing it");
																				exitCode = 0;
																			}
																			else
																			{
																				System.Console.Out.WriteLine("Could not fail task " + taskid);
																				exitCode = -1;
																			}
																		}
																	}
																	else
																	{
																		if (logs)
																		{
																			try
																			{
																				JobID jobID = JobID.ForName(jobid);
																				TaskAttemptID taskAttemptID = TaskAttemptID.ForName(taskid);
																				LogParams logParams = cluster.GetLogParams(jobID, taskAttemptID);
																				LogCLIHelpers logDumper = new LogCLIHelpers();
																				logDumper.SetConf(GetConf());
																				exitCode = logDumper.DumpAContainersLogs(logParams.GetApplicationId(), logParams.
																					GetContainerId(), logParams.GetNodeId(), logParams.GetOwner());
																			}
																			catch (IOException e)
																			{
																				if (e is RemoteException)
																				{
																					throw;
																				}
																				System.Console.Out.WriteLine(e.Message);
																			}
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			catch (RemoteException re)
			{
				IOException unwrappedException = re.UnwrapRemoteException();
				if (unwrappedException is AccessControlException)
				{
					System.Console.Out.WriteLine(unwrappedException.Message);
				}
				else
				{
					throw;
				}
			}
			finally
			{
				cluster.Close();
			}
			return exitCode;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual Cluster CreateCluster()
		{
			return new Cluster(GetConf());
		}

		private string GetJobPriorityNames()
		{
			StringBuilder sb = new StringBuilder();
			foreach (JobPriority p in JobPriority.Values())
			{
				sb.Append(p.ToString()).Append(" ");
			}
			return sb.Substring(0, sb.Length - 1);
		}

		private string GetTaskTypes()
		{
			return StringUtils.Join(taskTypes, " ");
		}

		/// <summary>Display usage of the command-line tool and terminate execution.</summary>
		private void DisplayUsage(string cmd)
		{
			string prefix = "Usage: CLI ";
			string jobPriorityValues = GetJobPriorityNames();
			string taskStates = "running, completed";
			if ("-submit".Equals(cmd))
			{
				System.Console.Error.WriteLine(prefix + "[" + cmd + " <job-file>]");
			}
			else
			{
				if ("-status".Equals(cmd) || "-kill".Equals(cmd))
				{
					System.Console.Error.WriteLine(prefix + "[" + cmd + " <job-id>]");
				}
				else
				{
					if ("-counter".Equals(cmd))
					{
						System.Console.Error.WriteLine(prefix + "[" + cmd + " <job-id> <group-name> <counter-name>]"
							);
					}
					else
					{
						if ("-events".Equals(cmd))
						{
							System.Console.Error.WriteLine(prefix + "[" + cmd + " <job-id> <from-event-#> <#-of-events>]. Event #s start from 1."
								);
						}
						else
						{
							if ("-history".Equals(cmd))
							{
								System.Console.Error.WriteLine(prefix + "[" + cmd + " <jobHistoryFile>]");
							}
							else
							{
								if ("-list".Equals(cmd))
								{
									System.Console.Error.WriteLine(prefix + "[" + cmd + " [all]]");
								}
								else
								{
									if ("-kill-task".Equals(cmd) || "-fail-task".Equals(cmd))
									{
										System.Console.Error.WriteLine(prefix + "[" + cmd + " <task-attempt-id>]");
									}
									else
									{
										if ("-set-priority".Equals(cmd))
										{
											System.Console.Error.WriteLine(prefix + "[" + cmd + " <job-id> <priority>]. " + "Valid values for priorities are: "
												 + jobPriorityValues);
										}
										else
										{
											if ("-list-active-trackers".Equals(cmd))
											{
												System.Console.Error.WriteLine(prefix + "[" + cmd + "]");
											}
											else
											{
												if ("-list-blacklisted-trackers".Equals(cmd))
												{
													System.Console.Error.WriteLine(prefix + "[" + cmd + "]");
												}
												else
												{
													if ("-list-attempt-ids".Equals(cmd))
													{
														System.Console.Error.WriteLine(prefix + "[" + cmd + " <job-id> <task-type> <task-state>]. "
															 + "Valid values for <task-type> are " + GetTaskTypes() + ". " + "Valid values for <task-state> are "
															 + taskStates);
													}
													else
													{
														if ("-logs".Equals(cmd))
														{
															System.Console.Error.WriteLine(prefix + "[" + cmd + " <job-id> <task-attempt-id>]. "
																 + " <task-attempt-id> is optional to get task attempt logs.");
														}
														else
														{
															System.Console.Error.Printf(prefix + "<command> <args>%n");
															System.Console.Error.Printf("\t[-submit <job-file>]%n");
															System.Console.Error.Printf("\t[-status <job-id>]%n");
															System.Console.Error.Printf("\t[-counter <job-id> <group-name> <counter-name>]%n"
																);
															System.Console.Error.Printf("\t[-kill <job-id>]%n");
															System.Console.Error.Printf("\t[-set-priority <job-id> <priority>]. " + "Valid values for priorities are: "
																 + jobPriorityValues + "%n");
															System.Console.Error.Printf("\t[-events <job-id> <from-event-#> <#-of-events>]%n"
																);
															System.Console.Error.Printf("\t[-history <jobHistoryFile>]%n");
															System.Console.Error.Printf("\t[-list [all]]%n");
															System.Console.Error.Printf("\t[-list-active-trackers]%n");
															System.Console.Error.Printf("\t[-list-blacklisted-trackers]%n");
															System.Console.Error.WriteLine("\t[-list-attempt-ids <job-id> <task-type> " + "<task-state>]. "
																 + "Valid values for <task-type> are " + GetTaskTypes() + ". " + "Valid values for <task-state> are "
																 + taskStates);
															System.Console.Error.Printf("\t[-kill-task <task-attempt-id>]%n");
															System.Console.Error.Printf("\t[-fail-task <task-attempt-id>]%n");
															System.Console.Error.Printf("\t[-logs <job-id> <task-attempt-id>]%n%n");
															ToolRunner.PrintGenericCommandUsage(System.Console.Out);
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ViewHistory(string historyFile, bool all)
		{
			HistoryViewer historyViewer = new HistoryViewer(historyFile, GetConf(), all);
			historyViewer.Print();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual long GetCounter(Counters counters, string counterGroupName
			, string counterName)
		{
			return counters.FindCounter(counterGroupName, counterName).GetValue();
		}

		/// <summary>List the events for the given job</summary>
		/// <param name="jobId">the job id for the job's events to list</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void ListEvents(Job job, int fromEventId, int numEvents)
		{
			TaskCompletionEvent[] events = job.GetTaskCompletionEvents(fromEventId, numEvents
				);
			System.Console.Out.WriteLine("Task completion events for " + job.GetJobID());
			System.Console.Out.WriteLine("Number of events (from " + fromEventId + ") are: " 
				+ events.Length);
			foreach (TaskCompletionEvent @event in events)
			{
				System.Console.Out.WriteLine(@event.GetStatus() + " " + @event.GetTaskAttemptId()
					 + " " + GetTaskLogURL(@event.GetTaskAttemptId(), @event.GetTaskTrackerHttp()));
			}
		}

		protected internal static string GetTaskLogURL(TaskAttemptID taskId, string baseUrl
			)
		{
			return (baseUrl + "/tasklog?plaintext=true&attemptid=" + taskId);
		}

		/// <summary>Dump a list of currently running jobs</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void ListJobs(Cluster cluster)
		{
			IList<JobStatus> runningJobs = new AList<JobStatus>();
			foreach (JobStatus job in cluster.GetAllJobStatuses())
			{
				if (!job.IsJobComplete())
				{
					runningJobs.AddItem(job);
				}
			}
			DisplayJobList(Sharpen.Collections.ToArray(runningJobs, new JobStatus[0]));
		}

		/// <summary>Dump a list of all jobs submitted.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void ListAllJobs(Cluster cluster)
		{
			DisplayJobList(cluster.GetAllJobStatuses());
		}

		/// <summary>Display the list of active trackers</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void ListActiveTrackers(Cluster cluster)
		{
			TaskTrackerInfo[] trackers = cluster.GetActiveTaskTrackers();
			foreach (TaskTrackerInfo tracker in trackers)
			{
				System.Console.Out.WriteLine(tracker.GetTaskTrackerName());
			}
		}

		/// <summary>Display the list of blacklisted trackers</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void ListBlacklistedTrackers(Cluster cluster)
		{
			TaskTrackerInfo[] trackers = cluster.GetBlackListedTaskTrackers();
			if (trackers.Length > 0)
			{
				System.Console.Out.WriteLine("BlackListedNode \t Reason");
			}
			foreach (TaskTrackerInfo tracker in trackers)
			{
				System.Console.Out.WriteLine(tracker.GetTaskTrackerName() + "\t" + tracker.GetReasonForBlacklist
					());
			}
		}

		private void PrintTaskAttempts(TaskReport report)
		{
			if (report.GetCurrentStatus() == TIPStatus.Complete)
			{
				System.Console.Out.WriteLine(report.GetSuccessfulTaskAttemptId());
			}
			else
			{
				if (report.GetCurrentStatus() == TIPStatus.Running)
				{
					foreach (TaskAttemptID t in report.GetRunningTaskAttemptIds())
					{
						System.Console.Out.WriteLine(t);
					}
				}
			}
		}

		/// <summary>
		/// Display the information about a job's tasks, of a particular type and
		/// in a particular state
		/// </summary>
		/// <param name="job">the job</param>
		/// <param name="type">the type of the task (map/reduce/setup/cleanup)</param>
		/// <param name="state">
		/// the state of the task
		/// (pending/running/completed/failed/killed)
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual void DisplayTasks(Job job, string type, string state)
		{
			TaskReport[] reports = job.GetTaskReports(TaskType.ValueOf(StringUtils.ToUpperCase
				(type)));
			foreach (TaskReport report in reports)
			{
				TIPStatus status = report.GetCurrentStatus();
				if ((Sharpen.Runtime.EqualsIgnoreCase(state, "pending") && status == TIPStatus.Pending
					) || (Sharpen.Runtime.EqualsIgnoreCase(state, "running") && status == TIPStatus.
					Running) || (Sharpen.Runtime.EqualsIgnoreCase(state, "completed") && status == TIPStatus
					.Complete) || (Sharpen.Runtime.EqualsIgnoreCase(state, "failed") && status == TIPStatus
					.Failed) || (Sharpen.Runtime.EqualsIgnoreCase(state, "killed") && status == TIPStatus
					.Killed))
				{
					PrintTaskAttempts(report);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void DisplayJobList(JobStatus[] jobs)
		{
			DisplayJobList(jobs, new PrintWriter(new OutputStreamWriter(System.Console.Out, Charsets
				.Utf8)));
		}

		[InterfaceAudience.Private]
		public static string headerPattern = "%23s\t%10s\t%14s\t%12s\t%12s\t%10s\t%15s\t%15s\t%8s\t%8s\t%10s\t%10s\n";

		[InterfaceAudience.Private]
		public static string dataPattern = "%23s\t%10s\t%14d\t%12s\t%12s\t%10s\t%15s\t%15s\t%8s\t%8s\t%10s\t%10s\n";

		private static string memPattern = "%dM";

		private static string Unavailable = "N/A";

		[InterfaceAudience.Private]
		public virtual void DisplayJobList(JobStatus[] jobs, PrintWriter writer)
		{
			writer.WriteLine("Total jobs:" + jobs.Length);
			writer.Printf(headerPattern, "JobId", "State", "StartTime", "UserName", "Queue", 
				"Priority", "UsedContainers", "RsvdContainers", "UsedMem", "RsvdMem", "NeededMem"
				, "AM info");
			foreach (JobStatus job in jobs)
			{
				int numUsedSlots = job.GetNumUsedSlots();
				int numReservedSlots = job.GetNumReservedSlots();
				int usedMem = job.GetUsedMem();
				int rsvdMem = job.GetReservedMem();
				int neededMem = job.GetNeededMem();
				writer.Printf(dataPattern, job.GetJobID().ToString(), job.GetState(), job.GetStartTime
					(), job.GetUsername(), job.GetQueue(), job.GetPriority().ToString(), numUsedSlots
					 < 0 ? Unavailable : numUsedSlots, numReservedSlots < 0 ? Unavailable : numReservedSlots
					, usedMem < 0 ? Unavailable : string.Format(memPattern, usedMem), rsvdMem < 0 ? 
					Unavailable : string.Format(memPattern, rsvdMem), neededMem < 0 ? Unavailable : 
					string.Format(memPattern, neededMem), job.GetSchedulingInfo());
			}
			writer.Flush();
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int res = ToolRunner.Run(new CLI(), argv);
			ExitUtil.Terminate(res);
		}
	}
}
