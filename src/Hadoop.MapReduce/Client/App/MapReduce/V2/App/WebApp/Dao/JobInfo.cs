using System;
using System.Collections.Generic;
using System.Text;
using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class JobInfo
	{
		protected internal long startTime;

		protected internal long finishTime;

		protected internal long elapsedTime;

		protected internal string id;

		protected internal string name;

		protected internal string user;

		protected internal JobState state;

		protected internal int mapsTotal;

		protected internal int mapsCompleted;

		protected internal int reducesTotal;

		protected internal int reducesCompleted;

		protected internal float mapProgress;

		protected internal float reduceProgress;

		[XmlTransient]
		protected internal string mapProgressPercent;

		[XmlTransient]
		protected internal string reduceProgressPercent;

		protected internal int mapsPending;

		protected internal int mapsRunning;

		protected internal int reducesPending;

		protected internal int reducesRunning;

		protected internal bool uberized;

		protected internal string diagnostics;

		protected internal int newReduceAttempts = 0;

		protected internal int runningReduceAttempts = 0;

		protected internal int failedReduceAttempts = 0;

		protected internal int killedReduceAttempts = 0;

		protected internal int successfulReduceAttempts = 0;

		protected internal int newMapAttempts = 0;

		protected internal int runningMapAttempts = 0;

		protected internal int failedMapAttempts = 0;

		protected internal int killedMapAttempts = 0;

		protected internal int successfulMapAttempts = 0;

		protected internal AList<ConfEntryInfo> acls;

		public JobInfo()
		{
		}

		public JobInfo(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job, bool hasAccess)
		{
			// ok for any user to see
			// these should only be seen if acls allow
			this.id = MRApps.ToString(job.GetID());
			JobReport report = job.GetReport();
			this.startTime = report.GetStartTime();
			this.finishTime = report.GetFinishTime();
			this.elapsedTime = Times.Elapsed(this.startTime, this.finishTime);
			if (this.elapsedTime == -1)
			{
				this.elapsedTime = 0;
			}
			this.name = job.GetName().ToString();
			this.user = job.GetUserName();
			this.state = job.GetState();
			this.mapsTotal = job.GetTotalMaps();
			this.mapsCompleted = job.GetCompletedMaps();
			this.mapProgress = report.GetMapProgress() * 100;
			this.mapProgressPercent = StringHelper.Percent(report.GetMapProgress());
			this.reducesTotal = job.GetTotalReduces();
			this.reducesCompleted = job.GetCompletedReduces();
			this.reduceProgress = report.GetReduceProgress() * 100;
			this.reduceProgressPercent = StringHelper.Percent(report.GetReduceProgress());
			this.acls = new AList<ConfEntryInfo>();
			if (hasAccess)
			{
				this.diagnostics = string.Empty;
				CountTasksAndAttempts(job);
				this.uberized = job.IsUber();
				IList<string> diagnostics = job.GetDiagnostics();
				if (diagnostics != null && !diagnostics.IsEmpty())
				{
					StringBuilder b = new StringBuilder();
					foreach (string diag in diagnostics)
					{
						b.Append(diag);
					}
					this.diagnostics = b.ToString();
				}
				IDictionary<JobACL, AccessControlList> allacls = job.GetJobACLs();
				if (allacls != null)
				{
					foreach (KeyValuePair<JobACL, AccessControlList> entry in allacls)
					{
						this.acls.AddItem(new ConfEntryInfo(entry.Key.GetAclName(), entry.Value.GetAclString
							()));
					}
				}
			}
		}

		public virtual int GetNewReduceAttempts()
		{
			return this.newReduceAttempts;
		}

		public virtual int GetKilledReduceAttempts()
		{
			return this.killedReduceAttempts;
		}

		public virtual int GetFailedReduceAttempts()
		{
			return this.failedReduceAttempts;
		}

		public virtual int GetRunningReduceAttempts()
		{
			return this.runningReduceAttempts;
		}

		public virtual int GetSuccessfulReduceAttempts()
		{
			return this.successfulReduceAttempts;
		}

		public virtual int GetNewMapAttempts()
		{
			return this.newMapAttempts;
		}

		public virtual int GetKilledMapAttempts()
		{
			return this.killedMapAttempts;
		}

		public virtual AList<ConfEntryInfo> GetAcls()
		{
			return acls;
		}

		public virtual int GetFailedMapAttempts()
		{
			return this.failedMapAttempts;
		}

		public virtual int GetRunningMapAttempts()
		{
			return this.runningMapAttempts;
		}

		public virtual int GetSuccessfulMapAttempts()
		{
			return this.successfulMapAttempts;
		}

		public virtual int GetReducesCompleted()
		{
			return this.reducesCompleted;
		}

		public virtual int GetReducesTotal()
		{
			return this.reducesTotal;
		}

		public virtual int GetReducesPending()
		{
			return this.reducesPending;
		}

		public virtual int GetReducesRunning()
		{
			return this.reducesRunning;
		}

		public virtual int GetMapsCompleted()
		{
			return this.mapsCompleted;
		}

		public virtual int GetMapsTotal()
		{
			return this.mapsTotal;
		}

		public virtual int GetMapsPending()
		{
			return this.mapsPending;
		}

		public virtual int GetMapsRunning()
		{
			return this.mapsRunning;
		}

		public virtual string GetState()
		{
			return this.state.ToString();
		}

		public virtual string GetUserName()
		{
			return this.user;
		}

		public virtual string GetName()
		{
			return this.name;
		}

		public virtual string GetId()
		{
			return this.id;
		}

		public virtual long GetStartTime()
		{
			return this.startTime;
		}

		public virtual long GetElapsedTime()
		{
			return this.elapsedTime;
		}

		public virtual long GetFinishTime()
		{
			return this.finishTime;
		}

		public virtual bool IsUberized()
		{
			return this.uberized;
		}

		public virtual string Getdiagnostics()
		{
			return this.diagnostics;
		}

		public virtual float GetMapProgress()
		{
			return this.mapProgress;
		}

		public virtual string GetMapProgressPercent()
		{
			return this.mapProgressPercent;
		}

		public virtual float GetReduceProgress()
		{
			return this.reduceProgress;
		}

		public virtual string GetReduceProgressPercent()
		{
			return this.reduceProgressPercent;
		}

		/// <summary>
		/// Go through a job and update the member variables with counts for
		/// information to output in the page.
		/// </summary>
		/// <param name="job">the job to get counts for.</param>
		private void CountTasksAndAttempts(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job
			)
		{
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			if (tasks == null)
			{
				return;
			}
			foreach (Task task in tasks.Values)
			{
				switch (task.GetType())
				{
					case TaskType.Map:
					{
						switch (task.GetState())
						{
							case TaskState.Running:
							{
								// Task counts
								++this.mapsRunning;
								break;
							}

							case TaskState.Scheduled:
							{
								++this.mapsPending;
								break;
							}

							default:
							{
								break;
							}
						}
						break;
					}

					case TaskType.Reduce:
					{
						switch (task.GetState())
						{
							case TaskState.Running:
							{
								// Task counts
								++this.reducesRunning;
								break;
							}

							case TaskState.Scheduled:
							{
								++this.reducesPending;
								break;
							}

							default:
							{
								break;
							}
						}
						break;
					}

					default:
					{
						throw new InvalidOperationException("Task type is neither map nor reduce: " + task
							.GetType());
					}
				}
				// Attempts counts
				IDictionary<TaskAttemptId, TaskAttempt> attempts = task.GetAttempts();
				int newAttempts;
				int running;
				int successful;
				int failed;
				int killed;
				foreach (TaskAttempt attempt in attempts.Values)
				{
					newAttempts = 0;
					running = 0;
					successful = 0;
					failed = 0;
					killed = 0;
					if (MRApps.TaskAttemptStateUI.New.CorrespondsTo(attempt.GetState()))
					{
						++newAttempts;
					}
					else
					{
						if (MRApps.TaskAttemptStateUI.Running.CorrespondsTo(attempt.GetState()))
						{
							++running;
						}
						else
						{
							if (MRApps.TaskAttemptStateUI.Successful.CorrespondsTo(attempt.GetState()))
							{
								++successful;
							}
							else
							{
								if (MRApps.TaskAttemptStateUI.Failed.CorrespondsTo(attempt.GetState()))
								{
									++failed;
								}
								else
								{
									if (MRApps.TaskAttemptStateUI.Killed.CorrespondsTo(attempt.GetState()))
									{
										++killed;
									}
								}
							}
						}
					}
					switch (task.GetType())
					{
						case TaskType.Map:
						{
							this.newMapAttempts += newAttempts;
							this.runningMapAttempts += running;
							this.successfulMapAttempts += successful;
							this.failedMapAttempts += failed;
							this.killedMapAttempts += killed;
							break;
						}

						case TaskType.Reduce:
						{
							this.newReduceAttempts += newAttempts;
							this.runningReduceAttempts += running;
							this.successfulReduceAttempts += successful;
							this.failedReduceAttempts += failed;
							this.killedReduceAttempts += killed;
							break;
						}

						default:
						{
							throw new InvalidOperationException("Task type neither map nor reduce: " + task.GetType
								());
						}
					}
				}
			}
		}
	}
}
