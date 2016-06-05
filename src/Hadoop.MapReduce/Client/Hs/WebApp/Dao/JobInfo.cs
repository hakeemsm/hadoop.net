using System.Collections.Generic;
using System.Text;
using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao
{
	public class JobInfo
	{
		protected internal long submitTime;

		protected internal long startTime;

		protected internal long finishTime;

		protected internal string id;

		protected internal string name;

		protected internal string queue;

		protected internal string user;

		protected internal string state;

		protected internal int mapsTotal;

		protected internal int mapsCompleted;

		protected internal int reducesTotal;

		protected internal int reducesCompleted;

		protected internal bool uberized;

		protected internal string diagnostics;

		protected internal long avgMapTime;

		protected internal long avgReduceTime;

		protected internal long avgShuffleTime;

		protected internal long avgMergeTime;

		protected internal int failedReduceAttempts;

		protected internal int killedReduceAttempts;

		protected internal int successfulReduceAttempts;

		protected internal int failedMapAttempts;

		protected internal int killedMapAttempts;

		protected internal int successfulMapAttempts;

		protected internal AList<ConfEntryInfo> acls;

		[XmlTransient]
		protected internal int numMaps;

		[XmlTransient]
		protected internal int numReduces;

		public JobInfo()
		{
		}

		public JobInfo(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			this.id = MRApps.ToString(job.GetID());
			JobReport report = job.GetReport();
			this.mapsTotal = job.GetTotalMaps();
			this.mapsCompleted = job.GetCompletedMaps();
			this.reducesTotal = job.GetTotalReduces();
			this.reducesCompleted = job.GetCompletedReduces();
			this.submitTime = report.GetSubmitTime();
			this.startTime = report.GetStartTime();
			this.finishTime = report.GetFinishTime();
			this.name = job.GetName().ToString();
			this.queue = job.GetQueueName();
			this.user = job.GetUserName();
			this.state = job.GetState().ToString();
			this.acls = new AList<ConfEntryInfo>();
			if (job is CompletedJob)
			{
				avgMapTime = 0l;
				avgReduceTime = 0l;
				avgShuffleTime = 0l;
				avgMergeTime = 0l;
				failedReduceAttempts = 0;
				killedReduceAttempts = 0;
				successfulReduceAttempts = 0;
				failedMapAttempts = 0;
				killedMapAttempts = 0;
				successfulMapAttempts = 0;
				CountTasksAndAttempts(job);
				this.uberized = job.IsUber();
				this.diagnostics = string.Empty;
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

		public virtual long GetNumMaps()
		{
			return numMaps;
		}

		public virtual long GetNumReduces()
		{
			return numReduces;
		}

		public virtual long GetAvgMapTime()
		{
			return avgMapTime;
		}

		public virtual long GetAvgReduceTime()
		{
			return avgReduceTime;
		}

		public virtual long GetAvgShuffleTime()
		{
			return avgShuffleTime;
		}

		public virtual long GetAvgMergeTime()
		{
			return avgMergeTime;
		}

		public virtual int GetFailedReduceAttempts()
		{
			return failedReduceAttempts;
		}

		public virtual int GetKilledReduceAttempts()
		{
			return killedReduceAttempts;
		}

		public virtual int GetSuccessfulReduceAttempts()
		{
			return successfulReduceAttempts;
		}

		public virtual int GetFailedMapAttempts()
		{
			return failedMapAttempts;
		}

		public virtual int GetKilledMapAttempts()
		{
			return killedMapAttempts;
		}

		public virtual int GetSuccessfulMapAttempts()
		{
			return successfulMapAttempts;
		}

		public virtual AList<ConfEntryInfo> GetAcls()
		{
			return acls;
		}

		public virtual int GetReducesCompleted()
		{
			return this.reducesCompleted;
		}

		public virtual int GetReducesTotal()
		{
			return this.reducesTotal;
		}

		public virtual int GetMapsCompleted()
		{
			return this.mapsCompleted;
		}

		public virtual int GetMapsTotal()
		{
			return this.mapsTotal;
		}

		public virtual string GetState()
		{
			return this.state;
		}

		public virtual string GetUserName()
		{
			return this.user;
		}

		public virtual string GetName()
		{
			return this.name;
		}

		public virtual string GetQueueName()
		{
			return this.queue;
		}

		public virtual string GetId()
		{
			return this.id;
		}

		public virtual long GetSubmitTime()
		{
			return this.submitTime;
		}

		public virtual long GetStartTime()
		{
			return this.startTime;
		}

		public virtual long GetFinishTime()
		{
			return this.finishTime;
		}

		public virtual bool IsUber()
		{
			return this.uberized;
		}

		public virtual string GetDiagnostics()
		{
			return this.diagnostics;
		}

		/// <summary>
		/// Go through a job and update the member variables with counts for
		/// information to output in the page.
		/// </summary>
		/// <param name="job">the job to get counts for.</param>
		private void CountTasksAndAttempts(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job
			)
		{
			numReduces = 0;
			numMaps = 0;
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			if (tasks == null)
			{
				return;
			}
			foreach (Task task in tasks.Values)
			{
				// Attempts counts
				IDictionary<TaskAttemptId, TaskAttempt> attempts = task.GetAttempts();
				int successful;
				int failed;
				int killed;
				foreach (TaskAttempt attempt in attempts.Values)
				{
					successful = 0;
					failed = 0;
					killed = 0;
					if (MRApps.TaskAttemptStateUI.New.CorrespondsTo(attempt.GetState()))
					{
					}
					else
					{
						// Do Nothing
						if (MRApps.TaskAttemptStateUI.Running.CorrespondsTo(attempt.GetState()))
						{
						}
						else
						{
							// Do Nothing
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
							successfulMapAttempts += successful;
							failedMapAttempts += failed;
							killedMapAttempts += killed;
							if (attempt.GetState() == TaskAttemptState.Succeeded)
							{
								numMaps++;
								avgMapTime += (attempt.GetFinishTime() - attempt.GetLaunchTime());
							}
							break;
						}

						case TaskType.Reduce:
						{
							successfulReduceAttempts += successful;
							failedReduceAttempts += failed;
							killedReduceAttempts += killed;
							if (attempt.GetState() == TaskAttemptState.Succeeded)
							{
								numReduces++;
								avgShuffleTime += (attempt.GetShuffleFinishTime() - attempt.GetLaunchTime());
								avgMergeTime += attempt.GetSortFinishTime() - attempt.GetShuffleFinishTime();
								avgReduceTime += (attempt.GetFinishTime() - attempt.GetSortFinishTime());
							}
							break;
						}
					}
				}
			}
			if (numMaps > 0)
			{
				avgMapTime = avgMapTime / numMaps;
			}
			if (numReduces > 0)
			{
				avgReduceTime = avgReduceTime / numReduces;
				avgShuffleTime = avgShuffleTime / numReduces;
				avgMergeTime = avgMergeTime / numReduces;
			}
		}
	}
}
