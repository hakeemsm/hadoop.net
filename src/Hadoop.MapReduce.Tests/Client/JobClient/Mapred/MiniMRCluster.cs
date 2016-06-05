using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This class is an MR2 replacement for older MR1 MiniMRCluster, that was used
	/// by tests prior to MR2.
	/// </summary>
	/// <remarks>
	/// This class is an MR2 replacement for older MR1 MiniMRCluster, that was used
	/// by tests prior to MR2. This replacement class uses the new MiniMRYarnCluster
	/// in MR2 but provides the same old MR1 interface, so tests can be migrated from
	/// MR1 to MR2 with minimal changes.
	/// Due to major differences between MR1 and MR2, a number of methods are either
	/// unimplemented/unsupported or were re-implemented to provide wrappers around
	/// MR2 functionality.
	/// </remarks>
	[System.ObsoleteAttribute(@"Use MiniMRClientClusterFactory instead")]
	public class MiniMRCluster
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.MiniMRCluster
			));

		private MiniMRClientCluster mrClientCluster;

		public virtual string GetTaskTrackerLocalDir(int taskTracker)
		{
			throw new NotSupportedException();
		}

		public virtual string[] GetTaskTrackerLocalDirs(int taskTracker)
		{
			throw new NotSupportedException();
		}

		internal class JobTrackerRunner
		{
			internal JobTrackerRunner(MiniMRCluster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MiniMRCluster _enclosing;
			// Mock class
		}

		internal class TaskTrackerRunner
		{
			internal TaskTrackerRunner(MiniMRCluster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MiniMRCluster _enclosing;
			// Mock class
		}

		public virtual MiniMRCluster.JobTrackerRunner GetJobTrackerRunner()
		{
			throw new NotSupportedException();
		}

		internal virtual MiniMRCluster.TaskTrackerRunner GetTaskTrackerRunner(int id)
		{
			throw new NotSupportedException();
		}

		public virtual int GetNumTaskTrackers()
		{
			throw new NotSupportedException();
		}

		public virtual void SetInlineCleanupThreads()
		{
			throw new NotSupportedException();
		}

		public virtual void WaitUntilIdle()
		{
			throw new NotSupportedException();
		}

		private void WaitTaskTrackers()
		{
			throw new NotSupportedException();
		}

		public virtual int GetJobTrackerPort()
		{
			throw new NotSupportedException();
		}

		public virtual JobConf CreateJobConf()
		{
			JobConf jobConf = null;
			try
			{
				jobConf = new JobConf(mrClientCluster.GetConfig());
			}
			catch (IOException e)
			{
				Log.Error(e);
			}
			return jobConf;
		}

		public virtual JobConf CreateJobConf(JobConf conf)
		{
			JobConf jobConf = null;
			try
			{
				jobConf = new JobConf(mrClientCluster.GetConfig());
			}
			catch (IOException e)
			{
				Log.Error(e);
			}
			return jobConf;
		}

		internal static JobConf ConfigureJobConf(JobConf conf, string namenode, int jobTrackerPort
			, int jobTrackerInfoPort, UserGroupInformation ugi)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public MiniMRCluster(int numTaskTrackers, string namenode, int numDir, string[] racks
			, string[] hosts)
			: this(0, 0, numTaskTrackers, namenode, numDir, racks, hosts)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public MiniMRCluster(int numTaskTrackers, string namenode, int numDir, string[] racks
			, string[] hosts, JobConf conf)
			: this(0, 0, numTaskTrackers, namenode, numDir, racks, hosts, null, conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public MiniMRCluster(int numTaskTrackers, string namenode, int numDir)
			: this(0, 0, numTaskTrackers, namenode, numDir)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers
			, string namenode, int numDir)
			: this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers
			, string namenode, int numDir, string[] racks)
			: this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir, racks, 
				null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers
			, string namenode, int numDir, string[] racks, string[] hosts)
			: this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir, racks, 
				hosts, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers
			, string namenode, int numDir, string[] racks, string[] hosts, UserGroupInformation
			 ugi)
			: this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir, racks, 
				hosts, ugi, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers
			, string namenode, int numDir, string[] racks, string[] hosts, UserGroupInformation
			 ugi, JobConf conf)
			: this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir, racks, 
				hosts, ugi, conf, 0)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers
			, string namenode, int numDir, string[] racks, string[] hosts, UserGroupInformation
			 ugi, JobConf conf, int numTrackerToExclude)
			: this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir, racks, 
				hosts, ugi, conf, numTrackerToExclude, new Clock())
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers
			, string namenode, int numDir, string[] racks, string[] hosts, UserGroupInformation
			 ugi, JobConf conf, int numTrackerToExclude, Clock clock)
		{
			if (conf == null)
			{
				conf = new JobConf();
			}
			FileSystem.SetDefaultUri(conf, namenode);
			string identifier = this.GetType().Name + "_" + Sharpen.Extensions.ToString(new Random
				().Next(int.MaxValue));
			mrClientCluster = MiniMRClientClusterFactory.Create(this.GetType(), identifier, numTaskTrackers
				, conf);
		}

		public virtual UserGroupInformation GetUgi()
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual TaskCompletionEvent[] GetTaskCompletionEvents(JobID id, int from, 
			int max)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetJobPriority(JobID jobId, JobPriority priority)
		{
			throw new NotSupportedException();
		}

		public virtual JobPriority GetJobPriority(JobID jobId)
		{
			throw new NotSupportedException();
		}

		public virtual long GetJobFinishTime(JobID jobId)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void InitializeJob(JobID jobId)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual MapTaskCompletionEventsUpdate GetMapTaskCompletionEventsUpdates(int
			 index, JobID jobId, int max)
		{
			throw new NotSupportedException();
		}

		public virtual JobConf GetJobTrackerConf()
		{
			JobConf jobConf = null;
			try
			{
				jobConf = new JobConf(mrClientCluster.GetConfig());
			}
			catch (IOException e)
			{
				Log.Error(e);
			}
			return jobConf;
		}

		public virtual int GetFaultCount(string hostName)
		{
			throw new NotSupportedException();
		}

		public virtual void StartJobTracker()
		{
		}

		// Do nothing
		public virtual void StartJobTracker(bool wait)
		{
		}

		// Do nothing
		public virtual void StopJobTracker()
		{
		}

		// Do nothing
		public virtual void StopTaskTracker(int id)
		{
		}

		// Do nothing
		/// <exception cref="System.IO.IOException"/>
		public virtual void StartTaskTracker(string host, string rack, int idx, int numDir
			)
		{
		}

		// Do nothing
		internal virtual void AddTaskTracker(MiniMRCluster.TaskTrackerRunner taskTracker)
		{
			throw new NotSupportedException();
		}

		internal virtual int GetTaskTrackerID(string trackerName)
		{
			throw new NotSupportedException();
		}

		public virtual void Shutdown()
		{
			try
			{
				mrClientCluster.Stop();
			}
			catch (IOException e)
			{
				Log.Error(e);
			}
		}
	}
}
