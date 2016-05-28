using System;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>A Page the shows info about the history server</summary>
	public class HsAboutPage : HsView
	{
		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
		*/
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			//override the nav config from commonPReHead
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:0}");
		}

		/// <summary>The content of this page is the attempts block</summary>
		/// <returns>AttemptsBlock.class</returns>
		protected override Type Content()
		{
			HistoryInfo info = new HistoryInfo();
			Info("History Server").("BuildVersion", info.GetHadoopBuildVersion() + " on " + info
				.GetHadoopVersionBuiltOn()).("History Server started on", Times.Format(info.GetStartedOn
				()));
			return typeof(InfoBlock);
		}
	}
}
