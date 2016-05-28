using System;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class JobPage : AppView
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			string jobID = $(AMParams.JobId);
			Set(Title, jobID.IsEmpty() ? "Bad request: missing job ID" : StringHelper.Join("MapReduce Job "
				, $(AMParams.JobId)));
			CommonPreHead(html);
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:2}");
		}

		protected override Type Content()
		{
			return typeof(JobBlock);
		}
	}
}
