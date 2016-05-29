using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class FairSchedulerPage : RmView
	{
		internal const string Q = ".ui-state-default.ui-corner-all";

		internal const float QMaxWidth = 0.8f;

		internal const float QStatsPos = QMaxWidth + 0.05f;

		internal const string QEnd = "left:101%";

		internal const string QGiven = "left:0%;background:none;border:1px solid rgba(0,0,0,1)";

		internal const string QInstantaneousFs = "left:0%;background:none;border:1px dashed rgba(0,0,0,1)";

		internal const string QOver = "background:rgba(255, 140, 0, 0.8)";

		internal const string QUnder = "background:rgba(50, 205, 50, 0.8)";

		internal const string SteadyFairShare = "Steady Fair Share";

		internal const string InstantaneousFairShare = "Instantaneous Fair Share";

		internal class FSQInfo
		{
			internal FairSchedulerQueueInfo qinfo;
		}

		internal class LeafQueueBlock : HtmlBlock
		{
			internal readonly FairSchedulerLeafQueueInfo qinfo;

			[Com.Google.Inject.Inject]
			internal LeafQueueBlock(View.ViewContext ctx, FairSchedulerPage.FSQInfo info)
				: base(ctx)
			{
				qinfo = (FairSchedulerLeafQueueInfo)info.qinfo;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				ResponseInfo ri = Info("\'" + qinfo.GetQueueName() + "\' Queue Status").("Used Resources:"
					, qinfo.GetUsedResources().ToString()).("Num Active Applications:", qinfo.GetNumActiveApplications
					()).("Num Pending Applications:", qinfo.GetNumPendingApplications()).("Min Resources:"
					, qinfo.GetMinResources().ToString()).("Max Resources:", qinfo.GetMaxResources()
					.ToString());
				int maxApps = qinfo.GetMaxApplications();
				if (maxApps < int.MaxValue)
				{
					ri.("Max Running Applications:", qinfo.GetMaxApplications());
				}
				ri.(SteadyFairShare + ":", qinfo.GetSteadyFairShare().ToString());
				ri.(InstantaneousFairShare + ":", qinfo.GetFairShare().ToString());
				html.(typeof(InfoBlock));
				// clear the info contents so this queue's info doesn't accumulate into another queue's info
				ri.Clear();
			}
		}

		internal class QueueBlock : HtmlBlock
		{
			internal readonly FairSchedulerPage.FSQInfo fsqinfo;

			[Com.Google.Inject.Inject]
			internal QueueBlock(FairSchedulerPage.FSQInfo info)
			{
				fsqinfo = info;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				ICollection<FairSchedulerQueueInfo> subQueues = fsqinfo.qinfo.GetChildQueues();
				Hamlet.UL<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> ul = html.Ul("#pq");
				foreach (FairSchedulerQueueInfo info in subQueues)
				{
					float capacity = info.GetMaxResourcesFraction();
					float steadyFairShare = info.GetSteadyFairShareMemoryFraction();
					float instantaneousFairShare = info.GetFairShareMemoryFraction();
					float used = info.GetUsedMemoryFraction();
					Hamlet.LI<Hamlet.UL<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> li = ul.Li().A(
						Q).$style(Width(capacity * QMaxWidth)).$title(StringHelper.Join(StringHelper.Join
						(SteadyFairShare + ":", Percent(steadyFairShare)), StringHelper.Join(" " + InstantaneousFairShare
						 + ":", Percent(instantaneousFairShare)))).Span().$style(StringHelper.Join(QGiven
						, ";font-size:1px;", Width(steadyFairShare / capacity))).('.').().Span().$style(
						StringHelper.Join(QInstantaneousFs, ";font-size:1px;", Width(instantaneousFairShare
						 / capacity))).('.').().Span().$style(StringHelper.Join(Width(used / capacity), 
						";font-size:1px;left:0%;", used > instantaneousFairShare ? QOver : QUnder)).('.'
						).().Span(".q", info.GetQueueName()).().Span().$class("qstats").$style(Left(QStatsPos
						)).(StringHelper.Join(Percent(used), " used")).();
					fsqinfo.qinfo = info;
					if (info is FairSchedulerLeafQueueInfo)
					{
						li.Ul("#lq").Li().(typeof(FairSchedulerPage.LeafQueueBlock)).().();
					}
					else
					{
						li.(typeof(FairSchedulerPage.QueueBlock));
					}
					li.();
				}
				ul.();
			}
		}

		internal class QueuesBlock : HtmlBlock
		{
			internal readonly FairScheduler fs;

			internal readonly FairSchedulerPage.FSQInfo fsqinfo;

			[Com.Google.Inject.Inject]
			internal QueuesBlock(ResourceManager rm, FairSchedulerPage.FSQInfo info)
			{
				fs = (FairScheduler)rm.GetResourceScheduler();
				fsqinfo = info;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				html.(typeof(MetricsOverviewTable));
				Hamlet.UL<Hamlet.DIV<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>> ul
					 = html.Div("#cs-wrapper.ui-widget").Div(".ui-widget-header.ui-corner-top").("Application Queues"
					).().Div("#cs.ui-widget-content.ui-corner-bottom").Ul();
				if (fs == null)
				{
					ul.Li().A(Q).$style(Width(QMaxWidth)).Span().$style(QEnd).("100% ").().Span(".q", 
						"default").().();
				}
				else
				{
					FairSchedulerInfo sinfo = new FairSchedulerInfo(fs);
					fsqinfo.qinfo = sinfo.GetRootQueueInfo();
					float used = fsqinfo.qinfo.GetUsedMemoryFraction();
					ul.Li().$style("margin-bottom: 1em").Span().$style("font-weight: bold").("Legend:"
						).().Span().$class("qlegend ui-corner-all").$style(QGiven).$title("The steady fair shares consider all queues, "
						 + "both active (with running applications) and inactive.").(SteadyFairShare).()
						.Span().$class("qlegend ui-corner-all").$style(QInstantaneousFs).$title("The instantaneous fair shares consider only active "
						 + "queues (with running applications).").(InstantaneousFairShare).().Span().$class
						("qlegend ui-corner-all").$style(QUnder).("Used").().Span().$class("qlegend ui-corner-all"
						).$style(QOver).("Used (over fair share)").().Span().$class("qlegend ui-corner-all ui-state-default"
						).("Max Capacity").().().Li().A(Q).$style(Width(QMaxWidth)).Span().$style(StringHelper.Join
						(Width(used), ";left:0%;", used > 1 ? QOver : QUnder)).(".").().Span(".q", "root"
						).().Span().$class("qstats").$style(Left(QStatsPos)).(StringHelper.Join(Percent(
						used), " used")).().(typeof(FairSchedulerPage.QueueBlock)).();
				}
				ul.().().Script().$type("text/javascript").("$('#cs').hide();").().().(typeof(FairSchedulerAppsBlock
					));
			}
		}

		protected override void PostHead(Hamlet.HTML<HtmlPage._> html)
		{
			html.Style().$type("text/css").("#cs { padding: 0.5em 0 1em 0; margin-bottom: 1em; position: relative }"
				, "#cs ul { list-style: none }", "#cs a { font-weight: normal; margin: 2px; position: relative }"
				, "#cs a span { font-weight: normal; font-size: 80% }", "#cs-wrapper .ui-widget-header { padding: 0.2em 0.5em }"
				, ".qstats { font-weight: normal; font-size: 80%; position: absolute }", ".qlegend { font-weight: normal; padding: 0 1em; margin: 1em }"
				, "table.info tr th {width: 50%}").().Script("/static/jt/jquery.jstree.js").Script
				().$type("text/javascript").("$(function() {", "  $('#cs a span').addClass('ui-corner-all').css('position', 'absolute');"
				, "  $('#cs').bind('loaded.jstree', function (e, data) {", "    var callback = { call:reopenQueryNodes }"
				, "    data.inst.open_node('#pq', callback);", "   }).", "    jstree({", "    core: { animation: 188, html_titles: true },"
				, "    plugins: ['themeroller', 'html_data', 'ui'],", "    themeroller: { item_open: 'ui-icon-minus',"
				, "      item_clsd: 'ui-icon-plus', item_leaf: 'ui-icon-gear'", "    }", "  });"
				, "  $('#cs').bind('select_node.jstree', function(e, data) {", "    var queues = $('.q', data.rslt.obj);"
				, "    var q = '^' + queues.first().text();", "    q += queues.length == 1 ? '$' : '\\\\.';"
				, "    $('#apps').dataTable().fnFilter(q, 4, true);", "  });", "  $('#cs').show();"
				, "});").().(typeof(SchedulerPageUtil.QueueBlockUtil));
		}

		// to center info table
		protected override Type Content()
		{
			return typeof(FairSchedulerPage.QueuesBlock);
		}

		protected internal override string InitAppsTable()
		{
			return WebPageUtils.AppsTableInit(true);
		}

		internal static string Percent(float f)
		{
			return string.Format("%.1f%%", f * 100);
		}

		internal static string Width(float f)
		{
			return string.Format("width:%.1f%%", f * 100);
		}

		internal static string Left(float f)
		{
			return string.Format("left:%.1f%%", f * 100);
		}
	}
}
