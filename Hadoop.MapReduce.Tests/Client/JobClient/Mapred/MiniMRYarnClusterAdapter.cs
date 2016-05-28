using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>An adapter for MiniMRYarnCluster providing a MiniMRClientCluster interface.
	/// 	</summary>
	/// <remarks>
	/// An adapter for MiniMRYarnCluster providing a MiniMRClientCluster interface.
	/// This interface could be used by tests across both MR1 and MR2.
	/// </remarks>
	public class MiniMRYarnClusterAdapter : MiniMRClientCluster
	{
		private MiniMRYarnCluster miniMRYarnCluster;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.MiniMRYarnClusterAdapter
			));

		public MiniMRYarnClusterAdapter(MiniMRYarnCluster miniMRYarnCluster)
		{
			this.miniMRYarnCluster = miniMRYarnCluster;
		}

		public virtual Configuration GetConfig()
		{
			return miniMRYarnCluster.GetConfig();
		}

		public virtual void Start()
		{
			miniMRYarnCluster.Start();
		}

		public virtual void Stop()
		{
			miniMRYarnCluster.Stop();
		}

		public virtual void Restart()
		{
			if (!miniMRYarnCluster.GetServiceState().Equals(Service.STATE.Started))
			{
				Log.Warn("Cannot restart the mini cluster, start it first");
				return;
			}
			Configuration oldConf = new Configuration(GetConfig());
			string callerName = oldConf.Get("minimrclientcluster.caller.name", this.GetType()
				.FullName);
			int noOfNMs = oldConf.GetInt("minimrclientcluster.nodemanagers.number", 1);
			oldConf.SetBoolean(YarnConfiguration.YarnMiniclusterFixedPorts, true);
			oldConf.SetBoolean(JHAdminConfig.MrHistoryMiniclusterFixedPorts, true);
			Stop();
			miniMRYarnCluster = new MiniMRYarnCluster(callerName, noOfNMs);
			miniMRYarnCluster.Init(oldConf);
			miniMRYarnCluster.Start();
		}
	}
}
