using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class AboutBlock : HtmlBlock
	{
		internal readonly ResourceManager rm;

		[Com.Google.Inject.Inject]
		internal AboutBlock(ResourceManager rm, View.ViewContext ctx)
			: base(ctx)
		{
			this.rm = rm;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			html.(typeof(MetricsOverviewTable));
			ResourceManager rm = GetInstance<ResourceManager>();
			ClusterInfo cinfo = new ClusterInfo(rm);
			Info("Cluster overview").("Cluster ID:", cinfo.GetClusterId()).("ResourceManager state:"
				, cinfo.GetState()).("ResourceManager HA state:", cinfo.GetHAState()).("ResourceManager HA zookeeper connection state:"
				, cinfo.GetHAZookeeperConnectionState()).("ResourceManager RMStateStore:", cinfo
				.GetRMStateStore()).("ResourceManager started on:", Times.Format(cinfo.GetStartedOn
				())).("ResourceManager version:", cinfo.GetRMBuildVersion() + " on " + cinfo.GetRMVersionBuiltOn
				()).("Hadoop version:", cinfo.GetHadoopBuildVersion() + " on " + cinfo.GetHadoopVersionBuiltOn
				());
			html.(typeof(InfoBlock));
		}
	}
}
