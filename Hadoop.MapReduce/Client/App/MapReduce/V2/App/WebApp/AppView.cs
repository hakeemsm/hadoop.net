using System;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class AppView : TwoColumnLayout
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			Set(DatatablesId, "jobs");
			Set(JQueryUI.InitID(Datatables, "jobs"), JobsTableInit());
			SetTableStyles(html, "jobs");
		}

		protected internal virtual void CommonPreHead(Hamlet.HTML<HtmlPage._> html)
		{
			Set(AccordionId, "nav");
			Set(JQueryUI.InitID(Accordion, "nav"), "{autoHeight:false, active:1}");
		}

		protected override Type Nav()
		{
			return typeof(NavBlock);
		}

		protected override Type Content()
		{
			return typeof(JobsBlock);
		}

		private string JobsTableInit()
		{
			return JQueryUI.TableInit().Append(", aaSorting: [[0, 'asc']]").Append(",aoColumns:[{sType:'title-numeric'},"
				).Append("null,null,{sType:'title-numeric', bSearchable:false},null,").Append("null,{sType:'title-numeric',bSearchable:false}, null, null]}"
				).ToString();
		}
		// Sort by id upon page load
	}
}
