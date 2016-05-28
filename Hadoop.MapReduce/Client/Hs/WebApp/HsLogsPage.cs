using System;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.Log;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	public class HsLogsPage : HsView
	{
		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
		*/
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			string logEntity = $(YarnWebParams.EntityString);
			if (logEntity == null || logEntity.IsEmpty())
			{
				logEntity = $(YarnWebParams.ContainerId);
			}
			if (logEntity == null || logEntity.IsEmpty())
			{
				logEntity = "UNKNOWN";
			}
			CommonPreHead(html);
		}

		/// <summary>The content of this page is the JobBlock</summary>
		/// <returns>HsJobBlock.class</returns>
		protected override Type Content()
		{
			return typeof(AggregatedLogsBlock);
		}
	}
}
