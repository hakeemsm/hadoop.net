using System;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class CountersPage : AppView
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			string tid = $(AMParams.TaskId);
			string activeNav = "3";
			if (tid == null || tid.IsEmpty())
			{
				activeNav = "2";
			}
			Set(JQueryUI.InitID(Accordion, "nav"), "{autoHeight:false, active:" + activeNav +
				 "}");
			Set(DatatablesSelector, "#counters .dt-counters");
			Set(JQueryUI.InitSelector(Datatables), "{bJQueryUI:true, sDom:'t', iDisplayLength:-1}"
				);
		}

		protected override void PostHead(Hamlet.HTML<HtmlPage._> html)
		{
			html.Style("#counters, .dt-counters { table-layout: fixed }", "#counters th { overflow: hidden; vertical-align: middle }"
				, "#counters .dataTables_wrapper { min-height: 1em }", "#counters .group { width: 15em }"
				, "#counters .name { width: 30em }");
		}

		protected override Type Content()
		{
			return typeof(CountersBlock);
		}
	}
}
