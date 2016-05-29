using System;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class NodeLabelsPage : RmView
	{
		internal class NodeLabelsBlock : HtmlBlock
		{
			internal readonly ResourceManager rm;

			[Com.Google.Inject.Inject]
			internal NodeLabelsBlock(ResourceManager rm, View.ViewContext ctx)
				: base(ctx)
			{
				this.rm = rm;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
					.Table("#nodelabels").Thead().Tr().Th(".name", "Label Name").Th(".numOfActiveNMs"
					, "Num Of Active NMs").Th(".totalResource", "Total Resource").().().Tbody();
				RMNodeLabelsManager nlm = rm.GetRMContext().GetNodeLabelManager();
				foreach (NodeLabel info in nlm.PullRMNodeLabelsInfo())
				{
					Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>
						> row = tbody.Tr().Td(info.GetLabelName().IsEmpty() ? "<NO_LABEL>" : info.GetLabelName
						());
					int nActiveNMs = info.GetNumActiveNMs();
					if (nActiveNMs > 0)
					{
						row = row.Td().A(Url("nodes", "?" + YarnWebParams.NodeLabel + "=" + info.GetLabelName
							()), nActiveNMs.ToString()).();
					}
					else
					{
						row = row.Td(nActiveNMs.ToString());
					}
					row.Td(info.GetResource().ToString()).();
				}
				tbody.().();
			}
		}

		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			string title = "Node labels of the cluster";
			SetTitle(title);
			Set(JQueryUI.DatatablesId, "nodelabels");
			SetTableStyles(html, "nodelabels", ".healthStatus {width:10em}", ".healthReport {width:10em}"
				);
		}

		protected override Type Content()
		{
			return typeof(NodeLabelsPage.NodeLabelsBlock);
		}
	}
}
