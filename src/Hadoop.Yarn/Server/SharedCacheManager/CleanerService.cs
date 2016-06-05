using System;
using System.IO;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>
	/// The cleaner service that maintains the shared cache area, and cleans up stale
	/// entries on a regular basis.
	/// </summary>
	public class CleanerService : CompositeService
	{
		/// <summary>
		/// The name of the global cleaner lock that the cleaner creates to indicate
		/// that a cleaning process is in progress.
		/// </summary>
		public const string GlobalCleanerPid = ".cleaner_pid";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.CleanerService
			));

		private Configuration conf;

		private CleanerMetrics metrics;

		private ScheduledExecutorService scheduledExecutor;

		private readonly SCMStore store;

		private readonly Lock cleanerTaskLock;

		public CleanerService(SCMStore store)
			: base("CleanerService")
		{
			this.store = store;
			this.cleanerTaskLock = new ReentrantLock();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.conf = conf;
			// create scheduler executor service that services the cleaner tasks
			// use 2 threads to accommodate the on-demand tasks and reduce the chance of
			// back-to-back runs
			ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat("Shared cache cleaner"
				).Build();
			scheduledExecutor = Executors.NewScheduledThreadPool(2, tf);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			if (!WriteGlobalCleanerPidFile())
			{
				throw new YarnException("The global cleaner pid file already exists! " + "It appears there is another CleanerService running in the cluster"
					);
			}
			this.metrics = CleanerMetrics.GetInstance();
			// Start dependent services (i.e. AppChecker)
			base.ServiceStart();
			Runnable task = CleanerTask.Create(conf, store, metrics, cleanerTaskLock);
			long periodInMinutes = GetPeriod(conf);
			scheduledExecutor.ScheduleAtFixedRate(task, GetInitialDelay(conf), periodInMinutes
				, TimeUnit.Minutes);
			Log.Info("Scheduled the shared cache cleaner task to run every " + periodInMinutes
				 + " minutes.");
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			Log.Info("Shutting down the background thread.");
			scheduledExecutor.ShutdownNow();
			try
			{
				if (scheduledExecutor.AwaitTermination(10, TimeUnit.Seconds))
				{
					Log.Info("The background thread stopped.");
				}
				else
				{
					Log.Warn("Gave up waiting for the cleaner task to shutdown.");
				}
			}
			catch (Exception e)
			{
				Log.Warn("The cleaner service was interrupted while shutting down the task.", e);
			}
			RemoveGlobalCleanerPidFile();
			base.ServiceStop();
		}

		/// <summary>Execute an on-demand cleaner task.</summary>
		protected internal virtual void RunCleanerTask()
		{
			Runnable task = CleanerTask.Create(conf, store, metrics, cleanerTaskLock);
			// this is a non-blocking call (it simply submits the task to the executor
			// queue and returns)
			this.scheduledExecutor.Execute(task);
		}

		/// <summary>
		/// To ensure there are not multiple instances of the SCM running on a given
		/// cluster, a global pid file is used.
		/// </summary>
		/// <remarks>
		/// To ensure there are not multiple instances of the SCM running on a given
		/// cluster, a global pid file is used. This file contains the hostname of the
		/// machine that owns the pid file.
		/// </remarks>
		/// <returns>true if the pid file was written, false otherwise</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private bool WriteGlobalCleanerPidFile()
		{
			string root = conf.Get(YarnConfiguration.SharedCacheRoot, YarnConfiguration.DefaultSharedCacheRoot
				);
			Path pidPath = new Path(root, GlobalCleanerPid);
			try
			{
				FileSystem fs = FileSystem.Get(this.conf);
				if (fs.Exists(pidPath))
				{
					return false;
				}
				FSDataOutputStream os = fs.Create(pidPath, false);
				// write the hostname and the process id in the global cleaner pid file
				string Id = ManagementFactory.GetRuntimeMXBean().GetName();
				os.WriteUTF(Id);
				os.Close();
				// add it to the delete-on-exit to ensure it gets deleted when the JVM
				// exits
				fs.DeleteOnExit(pidPath);
			}
			catch (IOException e)
			{
				throw new YarnException(e);
			}
			Log.Info("Created the global cleaner pid file at " + pidPath.ToString());
			return true;
		}

		private void RemoveGlobalCleanerPidFile()
		{
			try
			{
				FileSystem fs = FileSystem.Get(this.conf);
				string root = conf.Get(YarnConfiguration.SharedCacheRoot, YarnConfiguration.DefaultSharedCacheRoot
					);
				Path pidPath = new Path(root, GlobalCleanerPid);
				fs.Delete(pidPath, false);
				Log.Info("Removed the global cleaner pid file at " + pidPath.ToString());
			}
			catch (IOException e)
			{
				Log.Error("Unable to remove the global cleaner pid file! The file may need " + "to be removed manually."
					, e);
			}
		}

		private static int GetInitialDelay(Configuration conf)
		{
			int initialDelayInMinutes = conf.GetInt(YarnConfiguration.ScmCleanerInitialDelayMins
				, YarnConfiguration.DefaultScmCleanerInitialDelayMins);
			// negative value is invalid; use the default
			if (initialDelayInMinutes < 0)
			{
				throw new HadoopIllegalArgumentException("Negative initial delay value: " + initialDelayInMinutes
					 + ". The initial delay must be greater than zero.");
			}
			return initialDelayInMinutes;
		}

		private static int GetPeriod(Configuration conf)
		{
			int periodInMinutes = conf.GetInt(YarnConfiguration.ScmCleanerPeriodMins, YarnConfiguration
				.DefaultScmCleanerPeriodMins);
			// non-positive value is invalid; use the default
			if (periodInMinutes <= 0)
			{
				throw new HadoopIllegalArgumentException("Non-positive period value: " + periodInMinutes
					 + ". The cleaner period must be greater than or equal to zero.");
			}
			return periodInMinutes;
		}
	}
}
