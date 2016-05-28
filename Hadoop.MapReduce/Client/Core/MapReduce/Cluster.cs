using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Protocol;
using Org.Apache.Hadoop.Mapreduce.Security.Token.Delegation;
using Org.Apache.Hadoop.Mapreduce.Util;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>Provides a way to access information about the map/reduce cluster.</summary>
	public class Cluster
	{
		public enum JobTrackerStatus
		{
			Initializing,
			Running
		}

		private ClientProtocolProvider clientProtocolProvider;

		private ClientProtocol client;

		private UserGroupInformation ugi;

		private Configuration conf;

		private FileSystem fs = null;

		private Path sysDir = null;

		private Path stagingAreaDir = null;

		private Path jobHistoryDir = null;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Cluster
			));

		private static ServiceLoader<ClientProtocolProvider> frameworkLoader = ServiceLoader
			.Load<ClientProtocolProvider>();

		static Cluster()
		{
			ConfigUtil.LoadResources();
		}

		/// <exception cref="System.IO.IOException"/>
		public Cluster(Configuration conf)
			: this(null, conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public Cluster(IPEndPoint jobTrackAddr, Configuration conf)
		{
			this.conf = conf;
			this.ugi = UserGroupInformation.GetCurrentUser();
			Initialize(jobTrackAddr, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void Initialize(IPEndPoint jobTrackAddr, Configuration conf)
		{
			lock (frameworkLoader)
			{
				foreach (ClientProtocolProvider provider in frameworkLoader)
				{
					Log.Debug("Trying ClientProtocolProvider : " + provider.GetType().FullName);
					ClientProtocol clientProtocol = null;
					try
					{
						if (jobTrackAddr == null)
						{
							clientProtocol = provider.Create(conf);
						}
						else
						{
							clientProtocol = provider.Create(jobTrackAddr, conf);
						}
						if (clientProtocol != null)
						{
							clientProtocolProvider = provider;
							client = clientProtocol;
							Log.Debug("Picked " + provider.GetType().FullName + " as the ClientProtocolProvider"
								);
							break;
						}
						else
						{
							Log.Debug("Cannot pick " + provider.GetType().FullName + " as the ClientProtocolProvider - returned null protocol"
								);
						}
					}
					catch (Exception e)
					{
						Log.Info("Failed to use " + provider.GetType().FullName + " due to error: ", e);
					}
				}
			}
			if (null == clientProtocolProvider || null == client)
			{
				throw new IOException("Cannot initialize Cluster. Please check your configuration for "
					 + MRConfig.FrameworkName + " and the correspond server addresses.");
			}
		}

		internal virtual ClientProtocol GetClient()
		{
			return client;
		}

		internal virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>Close the <code>Cluster</code>.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				clientProtocolProvider.Close(client);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private Job[] GetJobs(JobStatus[] stats)
		{
			IList<Job> jobs = new AList<Job>();
			foreach (JobStatus stat in stats)
			{
				jobs.AddItem(Job.GetInstance(this, stat, new JobConf(stat.GetJobFile())));
			}
			return Sharpen.Collections.ToArray(jobs, new Job[0]);
		}

		/// <summary>Get the file system where job-specific files are stored</summary>
		/// <returns>object of FileSystem</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual FileSystem GetFileSystem()
		{
			lock (this)
			{
				if (this.fs == null)
				{
					try
					{
						this.fs = ugi.DoAs(new _PrivilegedExceptionAction_162(this));
					}
					catch (Exception e)
					{
						throw new RuntimeException(e);
					}
				}
				return fs;
			}
		}

		private sealed class _PrivilegedExceptionAction_162 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_162(Cluster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				Path sysDir = new Path(this._enclosing.client.GetSystemDir());
				return sysDir.GetFileSystem(this._enclosing.GetConf());
			}

			private readonly Cluster _enclosing;
		}

		/// <summary>Get job corresponding to jobid.</summary>
		/// <param name="jobId"/>
		/// <returns>
		/// object of
		/// <see cref="Job"/>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual Job GetJob(JobID jobId)
		{
			JobStatus status = client.GetJobStatus(jobId);
			if (status != null)
			{
				JobConf conf;
				try
				{
					conf = new JobConf(status.GetJobFile());
				}
				catch (RuntimeException ex)
				{
					// If job file doesn't exist it means we can't find the job
					if (ex.InnerException is FileNotFoundException)
					{
						return null;
					}
					else
					{
						throw;
					}
				}
				return Job.GetInstance(this, status, conf);
			}
			return null;
		}

		/// <summary>Get all the queues in cluster.</summary>
		/// <returns>
		/// array of
		/// <see cref="QueueInfo"/>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual QueueInfo[] GetQueues()
		{
			return client.GetQueues();
		}

		/// <summary>Get queue information for the specified name.</summary>
		/// <param name="name">queuename</param>
		/// <returns>
		/// object of
		/// <see cref="QueueInfo"/>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual QueueInfo GetQueue(string name)
		{
			return client.GetQueue(name);
		}

		/// <summary>Get log parameters for the specified jobID or taskAttemptID</summary>
		/// <param name="jobID">the job id.</param>
		/// <param name="taskAttemptID">the task attempt id. Optional.</param>
		/// <returns>the LogParams</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual LogParams GetLogParams(JobID jobID, TaskAttemptID taskAttemptID)
		{
			return client.GetLogFileParams(jobID, taskAttemptID);
		}

		/// <summary>Get current cluster status.</summary>
		/// <returns>
		/// object of
		/// <see cref="ClusterMetrics"/>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual ClusterMetrics GetClusterStatus()
		{
			return client.GetClusterMetrics();
		}

		/// <summary>Get all active trackers in the cluster.</summary>
		/// <returns>
		/// array of
		/// <see cref="TaskTrackerInfo"/>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual TaskTrackerInfo[] GetActiveTaskTrackers()
		{
			return client.GetActiveTrackers();
		}

		/// <summary>Get blacklisted trackers.</summary>
		/// <returns>
		/// array of
		/// <see cref="TaskTrackerInfo"/>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual TaskTrackerInfo[] GetBlackListedTaskTrackers()
		{
			return client.GetBlacklistedTrackers();
		}

		/// <summary>Get all the jobs in cluster.</summary>
		/// <returns>
		/// array of
		/// <see cref="Job"/>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[System.ObsoleteAttribute(@"Use GetAllJobStatuses() instead.")]
		public virtual Job[] GetAllJobs()
		{
			return GetJobs(client.GetAllJobs());
		}

		/// <summary>Get job status for all jobs in the cluster.</summary>
		/// <returns>job status for all jobs in cluster</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual JobStatus[] GetAllJobStatuses()
		{
			return client.GetAllJobs();
		}

		/// <summary>
		/// Grab the jobtracker system directory path where
		/// job-specific files will  be placed.
		/// </summary>
		/// <returns>the system directory where job-specific files are to be placed.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual Path GetSystemDir()
		{
			if (sysDir == null)
			{
				sysDir = new Path(client.GetSystemDir());
			}
			return sysDir;
		}

		/// <summary>
		/// Grab the jobtracker's view of the staging directory path where
		/// job-specific files will  be placed.
		/// </summary>
		/// <returns>the staging directory where job-specific files are to be placed.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual Path GetStagingAreaDir()
		{
			if (stagingAreaDir == null)
			{
				stagingAreaDir = new Path(client.GetStagingAreaDir());
			}
			return stagingAreaDir;
		}

		/// <summary>Get the job history file path for a given job id.</summary>
		/// <remarks>
		/// Get the job history file path for a given job id. The job history file at
		/// this path may or may not be existing depending on the job completion state.
		/// The file is present only for the completed jobs.
		/// </remarks>
		/// <param name="jobId">the JobID of the job submitted by the current user.</param>
		/// <returns>the file path of the job history file</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual string GetJobHistoryUrl(JobID jobId)
		{
			if (jobHistoryDir == null)
			{
				jobHistoryDir = new Path(client.GetJobHistoryDir());
			}
			return new Path(jobHistoryDir, jobId.ToString() + "_" + ugi.GetShortUserName()).ToString
				();
		}

		/// <summary>Gets the Queue ACLs for current user</summary>
		/// <returns>array of QueueAclsInfo object for current user.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual QueueAclsInfo[] GetQueueAclsForCurrentUser()
		{
			return client.GetQueueAclsForCurrentUser();
		}

		/// <summary>Gets the root level queues.</summary>
		/// <returns>array of JobQueueInfo object.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual QueueInfo[] GetRootQueues()
		{
			return client.GetRootQueues();
		}

		/// <summary>Returns immediate children of queueName.</summary>
		/// <param name="queueName"/>
		/// <returns>array of JobQueueInfo which are children of queueName</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual QueueInfo[] GetChildQueues(string queueName)
		{
			return client.GetChildQueues(queueName);
		}

		/// <summary>Get the JobTracker's status.</summary>
		/// <returns>
		/// 
		/// <see cref="JobTrackerStatus"/>
		/// of the JobTracker
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual Cluster.JobTrackerStatus GetJobTrackerStatus()
		{
			return client.GetJobTrackerStatus();
		}

		/// <summary>Get the tasktracker expiry interval for the cluster</summary>
		/// <returns>the expiry interval in msec</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual long GetTaskTrackerExpiryInterval()
		{
			return client.GetTaskTrackerExpiryInterval();
		}

		/// <summary>Get a delegation token for the user from the JobTracker.</summary>
		/// <param name="renewer">the user who can renew the token</param>
		/// <returns>the new token</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> 
			GetDelegationToken(Text renewer)
		{
			// client has already set the service
			return client.GetDelegationToken(renewer);
		}

		/// <summary>Renew a delegation token</summary>
		/// <param name="token">the token to renew</param>
		/// <returns>the new expiration time</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Security.Token.Token{T}.Renew(Org.Apache.Hadoop.Conf.Configuration) instead"
			)]
		public virtual long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
			> token)
		{
			return token.Renew(GetConf());
		}

		/// <summary>Cancel a delegation token from the JobTracker</summary>
		/// <param name="token">the token to cancel</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Security.Token.Token{T}.Cancel(Org.Apache.Hadoop.Conf.Configuration) instead"
			)]
		public virtual void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token<
			DelegationTokenIdentifier> token)
		{
			token.Cancel(GetConf());
		}
	}
}
