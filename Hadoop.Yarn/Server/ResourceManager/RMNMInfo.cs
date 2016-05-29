using System.Collections.Generic;
using Javax.Management;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Mortbay.Util.Ajax;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	/// <summary>JMX bean listing statuses of all node managers.</summary>
	public class RMNMInfo : RMNMInfoBeans
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.RMNMInfo
			));

		private RMContext rmContext;

		private ResourceScheduler scheduler;

		/// <summary>Constructor for RMNMInfo registers the bean with JMX.</summary>
		/// <param name="rmc">resource manager's context object</param>
		/// <param name="sched">resource manager's scheduler object</param>
		public RMNMInfo(RMContext rmc, ResourceScheduler sched)
		{
			this.rmContext = rmc;
			this.scheduler = sched;
			StandardMBean bean;
			try
			{
				bean = new StandardMBean(this, typeof(RMNMInfoBeans));
				MBeans.Register("ResourceManager", "RMNMInfo", bean);
			}
			catch (NotCompliantMBeanException e)
			{
				Log.Warn("Error registering RMNMInfo MBean", e);
			}
			Log.Info("Registered RMNMInfo MBean");
		}

		[System.Serializable]
		internal class InfoMap : LinkedHashMap<string, object>
		{
			private const long serialVersionUID = 1L;
		}

		/// <summary>Implements getLiveNodeManagers()</summary>
		/// <returns>JSON formatted string containing statuses of all node managers</returns>
		public virtual string GetLiveNodeManagers()
		{
			// RMNMInfoBeans
			ICollection<RMNode> nodes = this.rmContext.GetRMNodes().Values;
			IList<RMNMInfo.InfoMap> nodesInfo = new AList<RMNMInfo.InfoMap>();
			foreach (RMNode ni in nodes)
			{
				SchedulerNodeReport report = scheduler.GetNodeReport(ni.GetNodeID());
				RMNMInfo.InfoMap info = new RMNMInfo.InfoMap();
				info["HostName"] = ni.GetHostName();
				info["Rack"] = ni.GetRackName();
				info["State"] = ni.GetState().ToString();
				info["NodeId"] = ni.GetNodeID();
				info["NodeHTTPAddress"] = ni.GetHttpAddress();
				info["LastHealthUpdate"] = ni.GetLastHealthReportTime();
				info["HealthReport"] = ni.GetHealthReport();
				info["NodeManagerVersion"] = ni.GetNodeManagerVersion();
				if (report != null)
				{
					info["NumContainers"] = report.GetNumContainers();
					info["UsedMemoryMB"] = report.GetUsedResource().GetMemory();
					info["AvailableMemoryMB"] = report.GetAvailableResource().GetMemory();
				}
				nodesInfo.AddItem(info);
			}
			return JSON.ToString(nodesInfo);
		}
	}
}
