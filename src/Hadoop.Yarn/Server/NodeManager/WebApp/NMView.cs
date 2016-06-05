using System;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class NMView : TwoColumnLayout
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
		}

		protected internal virtual void CommonPreHead(Hamlet.HTML<HtmlPage._> html)
		{
			Set(JQueryUI.AccordionId, "nav");
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:0}");
		}

		protected override Type Nav()
		{
			return typeof(NavBlock);
		}
	}
}
