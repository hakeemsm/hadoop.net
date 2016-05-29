using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	/// <summary>
	/// The class which provides functionality of checking the health of the node and
	/// reporting back to the service for which the health checker has been asked to
	/// report.
	/// </summary>
	public class NodeHealthCheckerService : CompositeService
	{
		private NodeHealthScriptRunner nodeHealthScriptRunner;

		private LocalDirsHandlerService dirsHandler;

		internal const string Separator = ";";

		public NodeHealthCheckerService()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeHealthCheckerService)
				.FullName)
		{
			dirsHandler = new LocalDirsHandlerService();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			if (NodeHealthScriptRunner.ShouldRun(conf))
			{
				nodeHealthScriptRunner = new NodeHealthScriptRunner();
				AddService(nodeHealthScriptRunner);
			}
			AddService(dirsHandler);
			base.ServiceInit(conf);
		}

		/// <returns>the reporting string of health of the node</returns>
		internal virtual string GetHealthReport()
		{
			string scriptReport = (nodeHealthScriptRunner == null) ? string.Empty : nodeHealthScriptRunner
				.GetHealthReport();
			if (scriptReport.Equals(string.Empty))
			{
				return dirsHandler.GetDisksHealthReport(false);
			}
			else
			{
				return scriptReport.Concat(Separator + dirsHandler.GetDisksHealthReport(false));
			}
		}

		/// <returns><em>true</em> if the node is healthy</returns>
		internal virtual bool IsHealthy()
		{
			bool scriptHealthStatus = (nodeHealthScriptRunner == null) ? true : nodeHealthScriptRunner
				.IsHealthy();
			return scriptHealthStatus && dirsHandler.AreDisksHealthy();
		}

		/// <returns>when the last time the node health status is reported</returns>
		internal virtual long GetLastHealthReportTime()
		{
			long diskCheckTime = dirsHandler.GetLastDisksCheckTime();
			long lastReportTime = (nodeHealthScriptRunner == null) ? diskCheckTime : Math.Max
				(nodeHealthScriptRunner.GetLastReportedTime(), diskCheckTime);
			return lastReportTime;
		}

		/// <returns>the disk handler</returns>
		public virtual LocalDirsHandlerService GetDiskHandler()
		{
			return dirsHandler;
		}

		/// <returns>the node health script runner</returns>
		internal virtual NodeHealthScriptRunner GetNodeHealthScriptRunner()
		{
			return nodeHealthScriptRunner;
		}
	}
}
