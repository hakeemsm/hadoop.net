using System;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.Log;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp
{
	public class AHSLogsPage : AHSView
	{
		/*
		* (non-Javadoc)
		*
		* @see
		* org.apache.hadoop.yarn.server.applicationhistoryservice.webapp.AHSView#
		* preHead(org.apache.hadoop .yarn.webapp.hamlet.Hamlet.HTML)
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

		/// <summary>The content of this page is the AggregatedLogsBlock</summary>
		/// <returns>AggregatedLogsBlock.class</returns>
		protected override Type Content()
		{
			return typeof(AggregatedLogsBlock);
		}
	}
}
