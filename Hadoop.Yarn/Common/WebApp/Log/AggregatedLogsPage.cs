using System;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Log
{
	public class AggregatedLogsPage : TwoColumnLayout
	{
		/* (non-Javadoc)
		* @see org.apache.hadoop.yarn.server.nodemanager.webapp.NMView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
		*/
		protected internal override void PreHead(Hamlet.HTML<HtmlPage._> html)
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
			Set(Title, StringHelper.Join("Logs for ", logEntity));
			Set(JQueryUI.AccordionId, "nav");
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:0}");
		}

		protected internal override Type Content()
		{
			return typeof(AggregatedLogsBlock);
		}

		protected internal override Type Nav()
		{
			return typeof(AggregatedLogsNavBlock);
		}
	}
}
