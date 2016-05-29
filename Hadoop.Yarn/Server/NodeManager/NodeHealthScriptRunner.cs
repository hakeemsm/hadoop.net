using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	/// <summary>
	/// The class which provides functionality of checking the health of the node
	/// using the configured node health script and reporting back to the service
	/// for which the health checker has been asked to report.
	/// </summary>
	public class NodeHealthScriptRunner : AbstractService
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeHealthScriptRunner
			));

		/// <summary>Absolute path to the health script.</summary>
		private string nodeHealthScript;

		/// <summary>Delay after which node health script to be executed</summary>
		private long intervalTime;

		/// <summary>Time after which the script should be timedout</summary>
		private long scriptTimeout;

		/// <summary>Timer used to schedule node health monitoring script execution</summary>
		private Timer nodeHealthScriptScheduler;

		/// <summary>ShellCommandExecutor used to execute monitoring script</summary>
		internal Shell.ShellCommandExecutor shexec = null;

		/// <summary>Configuration used by the checker</summary>
		private Configuration conf;

		/// <summary>Pattern used for searching in the output of the node health script</summary>
		private const string ErrorPattern = "ERROR";

		/// <summary>Time out error message</summary>
		internal const string NodeHealthScriptTimedOutMsg = "Node health script timed out";

		private bool isHealthy;

		private string healthReport;

		private long lastReportedTime;

		private TimerTask timer;

		private enum HealthCheckerExitStatus
		{
			Success,
			TimedOut,
			FailedWithExitCode,
			FailedWithException,
			Failed
		}

		/// <summary>
		/// Class which is used by the
		/// <see cref="Sharpen.Timer"/>
		/// class to periodically execute the
		/// node health script.
		/// </summary>
		private class NodeHealthMonitorExecutor : TimerTask
		{
			internal string exceptionStackTrace = string.Empty;

			public NodeHealthMonitorExecutor(NodeHealthScriptRunner _enclosing, string[] args
				)
			{
				this._enclosing = _enclosing;
				AList<string> execScript = new AList<string>();
				execScript.AddItem(this._enclosing.nodeHealthScript);
				if (args != null)
				{
					Sharpen.Collections.AddAll(execScript, Arrays.AsList(args));
				}
				this._enclosing.shexec = new Shell.ShellCommandExecutor(Sharpen.Collections.ToArray
					(execScript, new string[execScript.Count]), null, null, this._enclosing.scriptTimeout
					);
			}

			public override void Run()
			{
				NodeHealthScriptRunner.HealthCheckerExitStatus status = NodeHealthScriptRunner.HealthCheckerExitStatus
					.Success;
				try
				{
					this._enclosing.shexec.Execute();
				}
				catch (Shell.ExitCodeException)
				{
					// ignore the exit code of the script
					status = NodeHealthScriptRunner.HealthCheckerExitStatus.FailedWithExitCode;
					// On Windows, we will not hit the Stream closed IOException
					// thrown by stdout buffered reader for timeout event.
					if (Shell.Windows && this._enclosing.shexec.IsTimedOut())
					{
						status = NodeHealthScriptRunner.HealthCheckerExitStatus.TimedOut;
					}
				}
				catch (Exception e)
				{
					NodeHealthScriptRunner.Log.Warn("Caught exception : " + e.Message);
					if (!this._enclosing.shexec.IsTimedOut())
					{
						status = NodeHealthScriptRunner.HealthCheckerExitStatus.FailedWithException;
					}
					else
					{
						status = NodeHealthScriptRunner.HealthCheckerExitStatus.TimedOut;
					}
					this.exceptionStackTrace = StringUtils.StringifyException(e);
				}
				finally
				{
					if (status == NodeHealthScriptRunner.HealthCheckerExitStatus.Success)
					{
						if (this.HasErrors(this._enclosing.shexec.GetOutput()))
						{
							status = NodeHealthScriptRunner.HealthCheckerExitStatus.Failed;
						}
					}
					this.ReportHealthStatus(status);
				}
			}

			/// <summary>
			/// Method which is used to parse output from the node health monitor and
			/// send to the report address.
			/// </summary>
			/// <remarks>
			/// Method which is used to parse output from the node health monitor and
			/// send to the report address.
			/// The timed out script or script which causes IOException output is
			/// ignored.
			/// The node is marked unhealthy if
			/// <ol>
			/// <li>The node health script times out</li>
			/// <li>The node health scripts output has a line which begins with ERROR</li>
			/// <li>An exception is thrown while executing the script</li>
			/// </ol>
			/// If the script throws
			/// <see cref="System.IO.IOException"/>
			/// or
			/// <see cref="Org.Apache.Hadoop.Util.Shell.ExitCodeException"/>
			/// the
			/// output is ignored and node is left remaining healthy, as script might
			/// have syntax error.
			/// </remarks>
			/// <param name="status"/>
			internal virtual void ReportHealthStatus(NodeHealthScriptRunner.HealthCheckerExitStatus
				 status)
			{
				long now = Runtime.CurrentTimeMillis();
				switch (status)
				{
					case NodeHealthScriptRunner.HealthCheckerExitStatus.Success:
					{
						this._enclosing.SetHealthStatus(true, string.Empty, now);
						break;
					}

					case NodeHealthScriptRunner.HealthCheckerExitStatus.TimedOut:
					{
						this._enclosing.SetHealthStatus(false, NodeHealthScriptRunner.NodeHealthScriptTimedOutMsg
							);
						break;
					}

					case NodeHealthScriptRunner.HealthCheckerExitStatus.FailedWithException:
					{
						this._enclosing.SetHealthStatus(false, this.exceptionStackTrace);
						break;
					}

					case NodeHealthScriptRunner.HealthCheckerExitStatus.FailedWithExitCode:
					{
						this._enclosing.SetHealthStatus(true, string.Empty, now);
						break;
					}

					case NodeHealthScriptRunner.HealthCheckerExitStatus.Failed:
					{
						this._enclosing.SetHealthStatus(false, this._enclosing.shexec.GetOutput());
						break;
					}
				}
			}

			/// <summary>Method to check if the output string has line which begins with ERROR.</summary>
			/// <param name="output">string</param>
			/// <returns>true if output string has error pattern in it.</returns>
			private bool HasErrors(string output)
			{
				string[] splits = output.Split("\n");
				foreach (string split in splits)
				{
					if (split.StartsWith(NodeHealthScriptRunner.ErrorPattern))
					{
						return true;
					}
				}
				return false;
			}

			private readonly NodeHealthScriptRunner _enclosing;
		}

		public NodeHealthScriptRunner()
			: base(typeof(NodeHealthScriptRunner).FullName)
		{
			this.lastReportedTime = Runtime.CurrentTimeMillis();
			this.isHealthy = true;
			this.healthReport = string.Empty;
		}

		/*
		* Method which initializes the values for the script path and interval time.
		*/
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.conf = conf;
			this.nodeHealthScript = conf.Get(YarnConfiguration.NmHealthCheckScriptPath);
			this.intervalTime = conf.GetLong(YarnConfiguration.NmHealthCheckIntervalMs, YarnConfiguration
				.DefaultNmHealthCheckIntervalMs);
			this.scriptTimeout = conf.GetLong(YarnConfiguration.NmHealthCheckScriptTimeoutMs, 
				YarnConfiguration.DefaultNmHealthCheckScriptTimeoutMs);
			string[] args = conf.GetStrings(YarnConfiguration.NmHealthCheckScriptOpts, new string
				[] {  });
			timer = new NodeHealthScriptRunner.NodeHealthMonitorExecutor(this, args);
			base.ServiceInit(conf);
		}

		/// <summary>Method used to start the Node health monitoring.</summary>
		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			// if health script path is not configured don't start the thread.
			if (!ShouldRun(conf))
			{
				Log.Info("Not starting node health monitor");
				return;
			}
			nodeHealthScriptScheduler = new Timer("NodeHealthMonitor-Timer", true);
			// Start the timer task immediately and
			// then periodically at interval time.
			nodeHealthScriptScheduler.ScheduleAtFixedRate(timer, 0, intervalTime);
			base.ServiceStart();
		}

		/// <summary>Method used to terminate the node health monitoring service.</summary>
		protected override void ServiceStop()
		{
			if (!ShouldRun(conf))
			{
				return;
			}
			if (nodeHealthScriptScheduler != null)
			{
				nodeHealthScriptScheduler.Cancel();
			}
			if (shexec != null)
			{
				SystemProcess p = shexec.GetProcess();
				if (p != null)
				{
					p.Destroy();
				}
			}
		}

		/// <summary>Gets the if the node is healthy or not</summary>
		/// <returns>true if node is healthy</returns>
		public virtual bool IsHealthy()
		{
			return isHealthy;
		}

		/// <summary>Sets if the node is healhty or not considering disks' health also.</summary>
		/// <param name="isHealthy">if or not node is healthy</param>
		private void SetHealthy(bool isHealthy)
		{
			lock (this)
			{
				this.isHealthy = isHealthy;
			}
		}

		/// <summary>Returns output from health script.</summary>
		/// <remarks>
		/// Returns output from health script. if node is healthy then an empty string
		/// is returned.
		/// </remarks>
		/// <returns>output from health script</returns>
		public virtual string GetHealthReport()
		{
			return healthReport;
		}

		/// <summary>Sets the health report from the node health script.</summary>
		/// <remarks>
		/// Sets the health report from the node health script. Also set the disks'
		/// health info obtained from DiskHealthCheckerService.
		/// </remarks>
		/// <param name="healthReport"/>
		private void SetHealthReport(string healthReport)
		{
			lock (this)
			{
				this.healthReport = healthReport;
			}
		}

		/// <summary>Returns time stamp when node health script was last run.</summary>
		/// <returns>timestamp when node health script was last run</returns>
		public virtual long GetLastReportedTime()
		{
			return lastReportedTime;
		}

		/// <summary>Sets the last run time of the node health script.</summary>
		/// <param name="lastReportedTime"/>
		private void SetLastReportedTime(long lastReportedTime)
		{
			lock (this)
			{
				this.lastReportedTime = lastReportedTime;
			}
		}

		/// <summary>
		/// Method used to determine if or not node health monitoring service should be
		/// started or not.
		/// </summary>
		/// <remarks>
		/// Method used to determine if or not node health monitoring service should be
		/// started or not. Returns true if following conditions are met:
		/// <ol>
		/// <li>Path to Node health check script is not empty</li>
		/// <li>Node health check script file exists</li>
		/// </ol>
		/// </remarks>
		/// <param name="conf"/>
		/// <returns>true if node health monitoring service can be started.</returns>
		public static bool ShouldRun(Configuration conf)
		{
			string nodeHealthScript = conf.Get(YarnConfiguration.NmHealthCheckScriptPath);
			if (nodeHealthScript == null || nodeHealthScript.Trim().IsEmpty())
			{
				return false;
			}
			FilePath f = new FilePath(nodeHealthScript);
			return f.Exists() && FileUtil.CanExecute(f);
		}

		private void SetHealthStatus(bool isHealthy, string output)
		{
			lock (this)
			{
				this.SetHealthy(isHealthy);
				this.SetHealthReport(output);
			}
		}

		private void SetHealthStatus(bool isHealthy, string output, long time)
		{
			lock (this)
			{
				this.SetHealthStatus(isHealthy, output);
				this.SetLastReportedTime(time);
			}
		}

		/// <summary>Used only by tests to access the timer task directly</summary>
		/// <returns>the timer task</returns>
		internal virtual TimerTask GetTimerTask()
		{
			return timer;
		}
	}
}
