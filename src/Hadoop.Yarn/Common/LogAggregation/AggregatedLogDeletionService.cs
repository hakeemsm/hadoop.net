using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Logaggregation
{
	/// <summary>A service that periodically deletes aggregated logs.</summary>
	public class AggregatedLogDeletionService : AbstractService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Logaggregation.AggregatedLogDeletionService
			));

		private Timer timer = null;

		private long checkIntervalMsecs;

		private AggregatedLogDeletionService.LogDeletionTask task;

		internal class LogDeletionTask : TimerTask
		{
			private Configuration conf;

			private long retentionMillis;

			private string suffix = null;

			private Path remoteRootLogDir = null;

			private ApplicationClientProtocol rmClient = null;

			public LogDeletionTask(Configuration conf, long retentionSecs, ApplicationClientProtocol
				 rmClient)
			{
				this.conf = conf;
				this.retentionMillis = retentionSecs * 1000;
				this.suffix = LogAggregationUtils.GetRemoteNodeLogDirSuffix(conf);
				this.remoteRootLogDir = new Path(conf.Get(YarnConfiguration.NmRemoteAppLogDir, YarnConfiguration
					.DefaultNmRemoteAppLogDir));
				this.rmClient = rmClient;
			}

			public override void Run()
			{
				long cutoffMillis = Runtime.CurrentTimeMillis() - retentionMillis;
				Log.Info("aggregated log deletion started.");
				try
				{
					FileSystem fs = remoteRootLogDir.GetFileSystem(conf);
					foreach (FileStatus userDir in fs.ListStatus(remoteRootLogDir))
					{
						if (userDir.IsDirectory())
						{
							Path userDirPath = new Path(userDir.GetPath(), suffix);
							DeleteOldLogDirsFrom(userDirPath, cutoffMillis, fs, rmClient);
						}
					}
				}
				catch (IOException e)
				{
					LogIOException("Error reading root log dir this deletion " + "attempt is being aborted"
						, e);
				}
				Log.Info("aggregated log deletion finished.");
			}

			private static void DeleteOldLogDirsFrom(Path dir, long cutoffMillis, FileSystem 
				fs, ApplicationClientProtocol rmClient)
			{
				try
				{
					foreach (FileStatus appDir in fs.ListStatus(dir))
					{
						if (appDir.IsDirectory() && appDir.GetModificationTime() < cutoffMillis)
						{
							bool appTerminated = IsApplicationTerminated(ConverterUtils.ToApplicationId(appDir
								.GetPath().GetName()), rmClient);
							if (appTerminated && ShouldDeleteLogDir(appDir, cutoffMillis, fs))
							{
								try
								{
									Log.Info("Deleting aggregated logs in " + appDir.GetPath());
									fs.Delete(appDir.GetPath(), true);
								}
								catch (IOException e)
								{
									LogIOException("Could not delete " + appDir.GetPath(), e);
								}
							}
							else
							{
								if (!appTerminated)
								{
									try
									{
										foreach (FileStatus node in fs.ListStatus(appDir.GetPath()))
										{
											if (node.GetModificationTime() < cutoffMillis)
											{
												try
												{
													fs.Delete(node.GetPath(), true);
												}
												catch (IOException ex)
												{
													LogIOException("Could not delete " + appDir.GetPath(), ex);
												}
											}
										}
									}
									catch (IOException e)
									{
										LogIOException("Error reading the contents of " + appDir.GetPath(), e);
									}
								}
							}
						}
					}
				}
				catch (IOException e)
				{
					LogIOException("Could not read the contents of " + dir, e);
				}
			}

			private static bool ShouldDeleteLogDir(FileStatus dir, long cutoffMillis, FileSystem
				 fs)
			{
				bool shouldDelete = true;
				try
				{
					foreach (FileStatus node in fs.ListStatus(dir.GetPath()))
					{
						if (node.GetModificationTime() >= cutoffMillis)
						{
							shouldDelete = false;
							break;
						}
					}
				}
				catch (IOException e)
				{
					LogIOException("Error reading the contents of " + dir.GetPath(), e);
					shouldDelete = false;
				}
				return shouldDelete;
			}

			/// <exception cref="System.IO.IOException"/>
			private static bool IsApplicationTerminated(ApplicationId appId, ApplicationClientProtocol
				 rmClient)
			{
				ApplicationReport appReport = null;
				try
				{
					appReport = rmClient.GetApplicationReport(GetApplicationReportRequest.NewInstance
						(appId)).GetApplicationReport();
				}
				catch (ApplicationNotFoundException)
				{
					return true;
				}
				catch (YarnException e)
				{
					throw new IOException(e);
				}
				YarnApplicationState currentState = appReport.GetYarnApplicationState();
				return currentState == YarnApplicationState.Failed || currentState == YarnApplicationState
					.Killed || currentState == YarnApplicationState.Finished;
			}

			public virtual ApplicationClientProtocol GetRMClient()
			{
				return this.rmClient;
			}
		}

		private static void LogIOException(string comment, IOException e)
		{
			if (e is AccessControlException)
			{
				string message = e.Message;
				//TODO fix this after HADOOP-8661
				message = message.Split("\n")[0];
				Log.Warn(comment + " " + message);
			}
			else
			{
				Log.Error(comment, e);
			}
		}

		public AggregatedLogDeletionService()
			: base(typeof(AggregatedLogDeletionService).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			ScheduleLogDeletionTask();
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			StopRMClient();
			StopTimer();
			base.ServiceStop();
		}

		private void SetLogAggCheckIntervalMsecs(long retentionSecs)
		{
			Configuration conf = GetConfig();
			checkIntervalMsecs = 1000 * conf.GetLong(YarnConfiguration.LogAggregationRetainCheckIntervalSeconds
				, YarnConfiguration.DefaultLogAggregationRetainCheckIntervalSeconds);
			if (checkIntervalMsecs <= 0)
			{
				// when unspecified compute check interval as 1/10th of retention
				checkIntervalMsecs = (retentionSecs * 1000) / 10;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshLogRetentionSettings()
		{
			if (GetServiceState() == Service.STATE.Started)
			{
				Configuration conf = CreateConf();
				SetConfig(conf);
				StopRMClient();
				StopTimer();
				ScheduleLogDeletionTask();
			}
			else
			{
				Log.Warn("Failed to execute refreshLogRetentionSettings : Aggregated Log Deletion Service is not started"
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ScheduleLogDeletionTask()
		{
			Configuration conf = GetConfig();
			if (!conf.GetBoolean(YarnConfiguration.LogAggregationEnabled, YarnConfiguration.DefaultLogAggregationEnabled
				))
			{
				// Log aggregation is not enabled so don't bother
				return;
			}
			long retentionSecs = conf.GetLong(YarnConfiguration.LogAggregationRetainSeconds, 
				YarnConfiguration.DefaultLogAggregationRetainSeconds);
			if (retentionSecs < 0)
			{
				Log.Info("Log Aggregation deletion is disabled because retention is" + " too small ("
					 + retentionSecs + ")");
				return;
			}
			SetLogAggCheckIntervalMsecs(retentionSecs);
			task = new AggregatedLogDeletionService.LogDeletionTask(conf, retentionSecs, CreatRMClient
				());
			timer = new Timer();
			timer.ScheduleAtFixedRate(task, 0, checkIntervalMsecs);
		}

		private void StopTimer()
		{
			if (timer != null)
			{
				timer.Cancel();
			}
		}

		public virtual long GetCheckIntervalMsecs()
		{
			return checkIntervalMsecs;
		}

		protected internal virtual Configuration CreateConf()
		{
			return new Configuration();
		}

		// Directly create and use ApplicationClientProtocol.
		// We have already marked ApplicationClientProtocol.getApplicationReport
		// as @Idempotent, it will automatically take care of RM restart/failover.
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal virtual ApplicationClientProtocol CreatRMClient()
		{
			return ClientRMProxy.CreateRMProxy<ApplicationClientProtocol>(GetConfig());
		}

		[VisibleForTesting]
		protected internal virtual void StopRMClient()
		{
			if (task != null && task.GetRMClient() != null)
			{
				RPC.StopProxy(task.GetRMClient());
			}
		}
	}
}
