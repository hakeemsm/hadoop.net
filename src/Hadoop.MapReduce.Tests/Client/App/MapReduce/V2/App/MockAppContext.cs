using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class MockAppContext : AppContext
	{
		internal readonly ApplicationAttemptId appAttemptID;

		internal readonly ApplicationId appID;

		internal readonly string user = MockJobs.NewUserName();

		internal readonly IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> 
			jobs;

		internal readonly long startTime = Runtime.CurrentTimeMillis();

		internal ICollection<string> blacklistedNodes;

		internal string queue;

		public MockAppContext(int appid)
		{
			appID = MockJobs.NewAppID(appid);
			appAttemptID = ApplicationAttemptId.NewInstance(appID, 0);
			jobs = null;
		}

		public MockAppContext(int appid, int numTasks, int numAttempts, Path confPath)
		{
			appID = MockJobs.NewAppID(appid);
			appAttemptID = ApplicationAttemptId.NewInstance(appID, 0);
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> map = Maps.NewHashMap
				();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = MockJobs.NewJob(appID, 0, numTasks
				, numAttempts, confPath);
			map[job.GetID()] = job;
			jobs = map;
		}

		public MockAppContext(int appid, int numJobs, int numTasks, int numAttempts)
			: this(appid, numJobs, numTasks, numAttempts, false)
		{
		}

		public MockAppContext(int appid, int numJobs, int numTasks, int numAttempts, bool
			 hasFailedTasks)
		{
			appID = MockJobs.NewAppID(appid);
			appAttemptID = ApplicationAttemptId.NewInstance(appID, 0);
			jobs = MockJobs.NewJobs(appID, numJobs, numTasks, numAttempts, hasFailedTasks);
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return appAttemptID;
		}

		public virtual ApplicationId GetApplicationID()
		{
			return appID;
		}

		public virtual CharSequence GetUser()
		{
			return user;
		}

		public virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetJob(JobId jobID)
		{
			return jobs[jobID];
		}

		public virtual IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllJobs
			()
		{
			return jobs;
		}

		// OK
		public virtual EventHandler GetEventHandler()
		{
			return null;
		}

		public virtual Clock GetClock()
		{
			return null;
		}

		public virtual string GetApplicationName()
		{
			return "TestApp";
		}

		public virtual long GetStartTime()
		{
			return startTime;
		}

		public virtual ClusterInfo GetClusterInfo()
		{
			return null;
		}

		public virtual ICollection<string> GetBlacklistedNodes()
		{
			return blacklistedNodes;
		}

		public virtual void SetBlacklistedNodes(ICollection<string> blacklistedNodes)
		{
			this.blacklistedNodes = blacklistedNodes;
		}

		public virtual ClientToAMTokenSecretManager GetClientToAMTokenSecretManager()
		{
			// Not implemented
			return null;
		}

		public virtual bool IsLastAMRetry()
		{
			return false;
		}

		public virtual bool HasSuccessfullyUnregistered()
		{
			// bogus - Not Required
			return true;
		}

		public virtual string GetNMHostname()
		{
			// bogus - Not Required
			return null;
		}
	}
}
