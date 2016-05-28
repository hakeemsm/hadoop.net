using System;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>Render a page that describes a specific job.</summary>
	public class HsJobPage : HsView
	{
		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
		*/
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			string jobID = $(AMParams.JobId);
			Set(Title, jobID.IsEmpty() ? "Bad request: missing job ID" : StringHelper.Join("MapReduce Job "
				, $(AMParams.JobId)));
			CommonPreHead(html);
			//Override the nav config from the commonPreHead
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:1}");
		}

		/// <summary>The content of this page is the JobBlock</summary>
		/// <returns>HsJobBlock.class</returns>
		protected override Type Content()
		{
			return typeof(HsJobBlock);
		}
	}
}
