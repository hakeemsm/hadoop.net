using System;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class NodePage : NMView
	{
		private const long BytesInMb = 1024 * 1024;

		protected internal override void CommonPreHead(Hamlet.HTML<HtmlPage._> html)
		{
			base.CommonPreHead(html);
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:1}");
		}

		protected override Type Content()
		{
			return typeof(NodePage.NodeBlock);
		}

		public class NodeBlock : HtmlBlock
		{
			private readonly Context context;

			private readonly ResourceView resourceView;

			[Com.Google.Inject.Inject]
			public NodeBlock(Context context, ResourceView resourceView)
			{
				this.context = context;
				this.resourceView = resourceView;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				NodeInfo info = new NodeInfo(this.context, this.resourceView);
				Info("NodeManager information").("Total Vmem allocated for Containers", StringUtils
					.ByteDesc(info.GetTotalVmemAllocated() * BytesInMb)).("Vmem enforcement enabled"
					, info.IsVmemCheckEnabled()).("Total Pmem allocated for Container", StringUtils.
					ByteDesc(info.GetTotalPmemAllocated() * BytesInMb)).("Pmem enforcement enabled", 
					info.IsPmemCheckEnabled()).("Total VCores allocated for Containers", info.GetTotalVCoresAllocated
					().ToString()).("NodeHealthyStatus", info.GetHealthStatus()).("LastNodeHealthTime"
					, Sharpen.Extensions.CreateDate(info.GetLastNodeUpdateTime())).("NodeHealthReport"
					, info.GetHealthReport()).("Node Manager Version:", info.GetNMBuildVersion() + " on "
					 + info.GetNMVersionBuiltOn()).("Hadoop Version:", info.GetHadoopBuildVersion() 
					+ " on " + info.GetHadoopVersionBuiltOn());
				html.(typeof(InfoBlock));
			}
		}
	}
}
