using System;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>Render the counters page</summary>
	public class HsSingleCounterPage : HsView
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
			Set(DatatablesId, "singleCounter");
			Set(JQueryUI.InitID(Datatables, "singleCounter"), CounterTableInit());
			SetTableStyles(html, "singleCounter");
		}

		/// <returns>
		/// The end of a javascript map that is the jquery datatable
		/// configuration for the jobs table.  the Jobs table is assumed to be
		/// rendered by the class returned from
		/// <see cref="Content()"/>
		/// 
		/// </returns>
		private string CounterTableInit()
		{
			return JQueryUI.TableInit().Append(", aoColumnDefs:[").Append("{'sType':'title-numeric', 'aTargets': [ 1 ] }"
				).Append("]}").ToString();
		}

		/// <summary>The content of this page is the CountersBlock now.</summary>
		/// <returns>CountersBlock.class</returns>
		protected override Type Content()
		{
			return typeof(SingleCounterBlock);
		}
	}
}
