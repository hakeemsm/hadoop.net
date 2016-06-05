using System;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>Render the counters page</summary>
	public class HsCountersPage : HsView
	{
		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
		*/
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			string tid = $(AMParams.TaskId);
			string activeNav = "2";
			if (tid == null || tid.IsEmpty())
			{
				activeNav = "1";
			}
			Set(JQueryUI.InitID(Accordion, "nav"), "{autoHeight:false, active:" + activeNav +
				 "}");
			Set(DatatablesSelector, "#counters .dt-counters");
			Set(JQueryUI.InitSelector(Datatables), "{bJQueryUI:true, sDom:'t', iDisplayLength:-1}"
				);
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#postHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
		*/
		protected override void PostHead(Hamlet.HTML<HtmlPage._> html)
		{
			html.Style("#counters, .dt-counters { table-layout: fixed }", "#counters th { overflow: hidden; vertical-align: middle }"
				, "#counters .dataTables_wrapper { min-height: 1em }", "#counters .group { width: 15em }"
				, "#counters .name { width: 30em }");
		}

		/// <summary>The content of this page is the CountersBlock now.</summary>
		/// <returns>CountersBlock.class</returns>
		protected override Type Content()
		{
			return typeof(CountersBlock);
		}
	}
}
