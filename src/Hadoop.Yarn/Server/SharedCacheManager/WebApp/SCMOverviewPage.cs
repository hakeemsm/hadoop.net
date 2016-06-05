using System;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Webapp
{
	/// <summary>This class is to render the shared cache manager web ui overview page.</summary>
	public class SCMOverviewPage : TwoColumnLayout
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			Set(JQueryUI.AccordionId, "nav");
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:0}");
		}

		protected override Type Content()
		{
			return typeof(SCMOverviewPage.SCMOverviewBlock);
		}

		protected override Type Nav()
		{
			return typeof(SCMOverviewPage.SCMOverviewNavBlock);
		}

		private class SCMOverviewNavBlock : HtmlBlock
		{
			protected override void Render(HtmlBlock.Block html)
			{
				html.Div("#nav").H3("Tools").Ul().Li().A("/conf", "Configuration").().Li().A("/stacks"
					, "Thread dump").().Li().A("/logs", "Logs").().Li().A("/metrics", "Metrics").().
					().();
			}
		}

		private class SCMOverviewBlock : HtmlBlock
		{
			internal readonly SharedCacheManager scm;

			[Com.Google.Inject.Inject]
			internal SCMOverviewBlock(SharedCacheManager scm, View.ViewContext ctx)
				: base(ctx)
			{
				this.scm = scm;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				SCMMetricsInfo metricsInfo = new SCMMetricsInfo(CleanerMetrics.GetInstance(), ClientSCMMetrics
					.GetInstance(), SharedCacheUploaderMetrics.GetInstance());
				Info("Shared Cache Manager overview").("Started on:", Times.Format(scm.GetStartTime
					())).("Cache hits: ", metricsInfo.GetCacheHits()).("Cache misses: ", metricsInfo
					.GetCacheMisses()).("Cache releases: ", metricsInfo.GetCacheReleases()).("Accepted uploads: "
					, metricsInfo.GetAcceptedUploads()).("Rejected uploads: ", metricsInfo.GetRejectUploads
					()).("Deleted files by the cleaner: ", metricsInfo.GetTotalDeletedFiles()).("Processed files by the cleaner: "
					, metricsInfo.GetTotalProcessedFiles());
				html.(typeof(InfoBlock));
			}
		}
	}
}
