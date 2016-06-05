using System;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	internal class CapacitySchedulerPage : RmView
	{
		internal const string Q = ".ui-state-default.ui-corner-all";

		internal const float QMaxWidth = 0.8f;

		internal const float QStatsPos = QMaxWidth + 0.05f;

		internal const string QEnd = "left:101%";

		internal const string QGiven = "left:0%;background:none;border:1px dashed rgba(0,0,0,0.25)";

		internal const string QOver = "background:rgba(255, 140, 0, 0.8)";

		internal const string QUnder = "background:rgba(50, 205, 50, 0.8)";

		internal class CSQInfo
		{
			internal CapacitySchedulerInfo csinfo;

			internal CapacitySchedulerQueueInfo qinfo;
		}

		internal class LeafQueueInfoBlock : HtmlBlock
		{
			internal readonly CapacitySchedulerLeafQueueInfo lqinfo;

			[Com.Google.Inject.Inject]
			internal LeafQueueInfoBlock(View.ViewContext ctx, CapacitySchedulerPage.CSQInfo info
				)
				: base(ctx)
			{
				lqinfo = (CapacitySchedulerLeafQueueInfo)info.qinfo;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				ResponseInfo ri = Info("\'" + Sharpen.Runtime.Substring(lqinfo.GetQueuePath(), 5)
					 + "\' Queue Status").("Queue State:", lqinfo.GetQueueState()).("Used Capacity:"
					, Percent(lqinfo.GetUsedCapacity() / 100)).("Absolute Used Capacity:", Percent(lqinfo
					.GetAbsoluteUsedCapacity() / 100)).("Absolute Capacity:", Percent(lqinfo.GetAbsoluteCapacity
					() / 100)).("Absolute Max Capacity:", Percent(lqinfo.GetAbsoluteMaxCapacity() / 
					100)).("Used Resources:", lqinfo.GetResourcesUsed().ToString()).("Num Schedulable Applications:"
					, Sharpen.Extensions.ToString(lqinfo.GetNumActiveApplications())).("Num Non-Schedulable Applications:"
					, Sharpen.Extensions.ToString(lqinfo.GetNumPendingApplications())).("Num Containers:"
					, Sharpen.Extensions.ToString(lqinfo.GetNumContainers())).("Max Applications:", 
					Sharpen.Extensions.ToString(lqinfo.GetMaxApplications())).("Max Applications Per User:"
					, Sharpen.Extensions.ToString(lqinfo.GetMaxApplicationsPerUser())).("Max Application Master Resources:"
					, lqinfo.GetAMResourceLimit().ToString()).("Used Application Master Resources:", 
					lqinfo.GetUsedAMResource().ToString()).("Max Application Master Resources Per User:"
					, lqinfo.GetUserAMResourceLimit().ToString()).("Configured Capacity:", Percent(lqinfo
					.GetCapacity() / 100)).("Configured Max Capacity:", Percent(lqinfo.GetMaxCapacity
					() / 100)).("Configured Minimum User Limit Percent:", Sharpen.Extensions.ToString
					(lqinfo.GetUserLimit()) + "%").("Configured User Limit Factor:", string.Format("%.1f"
					, lqinfo.GetUserLimitFactor())).("Accessible Node Labels:", StringUtils.Join(","
					, lqinfo.GetNodeLabels())).("Preemption:", lqinfo.GetPreemptionDisabled() ? "disabled"
					 : "enabled");
				html.(typeof(InfoBlock));
				// clear the info contents so this queue's info doesn't accumulate into another queue's info
				ri.Clear();
			}
		}

		internal class QueueUsersInfoBlock : HtmlBlock
		{
			internal readonly CapacitySchedulerLeafQueueInfo lqinfo;

			[Com.Google.Inject.Inject]
			internal QueueUsersInfoBlock(View.ViewContext ctx, CapacitySchedulerPage.CSQInfo 
				info)
				: base(ctx)
			{
				lqinfo = (CapacitySchedulerLeafQueueInfo)info.qinfo;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
					.Table("#userinfo").Thead().$class("ui-widget-header").Tr().Th().$class("ui-state-default"
					).("User Name").().Th().$class("ui-state-default").("Max Resource").().Th().$class
					("ui-state-default").("Used Resource").().Th().$class("ui-state-default").("Max AM Resource"
					).().Th().$class("ui-state-default").("Used AM Resource").().Th().$class("ui-state-default"
					).("Schedulable Apps").().Th().$class("ui-state-default").("Non-Schedulable Apps"
					).().().().Tbody();
				AList<UserInfo> users = lqinfo.GetUsers().GetUsersList();
				foreach (UserInfo userInfo in users)
				{
					tbody.Tr().Td(userInfo.GetUsername()).Td(userInfo.GetUserResourceLimit().ToString
						()).Td(userInfo.GetResourcesUsed().ToString()).Td(lqinfo.GetUserAMResourceLimit(
						).ToString()).Td(userInfo.GetAMResourcesUsed().ToString()).Td(Sharpen.Extensions.ToString
						(userInfo.GetNumActiveApplications())).Td(Sharpen.Extensions.ToString(userInfo.GetNumPendingApplications
						())).();
				}
				html.Div().$class("usersinfo").H5("Active Users Info").();
				tbody.().();
			}
		}

		public class QueueBlock : HtmlBlock
		{
			internal readonly CapacitySchedulerPage.CSQInfo csqinfo;

			[Com.Google.Inject.Inject]
			internal QueueBlock(CapacitySchedulerPage.CSQInfo info)
			{
				csqinfo = info;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				AList<CapacitySchedulerQueueInfo> subQueues = (csqinfo.qinfo == null) ? csqinfo.csinfo
					.GetQueues().GetQueueInfoList() : csqinfo.qinfo.GetQueues().GetQueueInfoList();
				Hamlet.UL<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> ul = html.Ul("#pq");
				foreach (CapacitySchedulerQueueInfo info in subQueues)
				{
					float used = info.GetUsedCapacity() / 100;
					float absCap = info.GetAbsoluteCapacity() / 100;
					float absMaxCap = info.GetAbsoluteMaxCapacity() / 100;
					float absUsedCap = info.GetAbsoluteUsedCapacity() / 100;
					Hamlet.LI<Hamlet.UL<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> li = ul.Li().A(
						Q).$style(Width(absMaxCap * QMaxWidth)).$title(StringHelper.Join("Absolute Capacity:"
						, Percent(absCap))).Span().$style(StringHelper.Join(QGiven, ";font-size:1px;", Width
						(absCap / absMaxCap))).('.').().Span().$style(StringHelper.Join(Width(absUsedCap
						 / absMaxCap), ";font-size:1px;left:0%;", absUsedCap > absCap ? QOver : QUnder))
						.('.').().Span(".q", Sharpen.Runtime.Substring(info.GetQueuePath(), 5)).().Span(
						).$class("qstats").$style(Left(QStatsPos)).(StringHelper.Join(Percent(used), " used"
						)).();
					csqinfo.qinfo = info;
					if (info.GetQueues() == null)
					{
						li.Ul("#lq").Li().(typeof(CapacitySchedulerPage.LeafQueueInfoBlock)).().();
						li.Ul("#lq").Li().(typeof(CapacitySchedulerPage.QueueUsersInfoBlock)).().();
					}
					else
					{
						li.(typeof(CapacitySchedulerPage.QueueBlock));
					}
					li.();
				}
				ul.();
			}
		}

		internal class QueuesBlock : HtmlBlock
		{
			internal readonly CapacityScheduler cs;

			internal readonly CapacitySchedulerPage.CSQInfo csqinfo;

			[Com.Google.Inject.Inject]
			internal QueuesBlock(ResourceManager rm, CapacitySchedulerPage.CSQInfo info)
			{
				cs = (CapacityScheduler)rm.GetResourceScheduler();
				csqinfo = info;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				html.(typeof(MetricsOverviewTable));
				Hamlet.UL<Hamlet.DIV<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>> ul
					 = html.Div("#cs-wrapper.ui-widget").Div(".ui-widget-header.ui-corner-top").("Application Queues"
					).().Div("#cs.ui-widget-content.ui-corner-bottom").Ul();
				if (cs == null)
				{
					ul.Li().A(Q).$style(Width(QMaxWidth)).Span().$style(QEnd).("100% ").().Span(".q", 
						"default").().();
				}
				else
				{
					CSQueue root = cs.GetRootQueue();
					CapacitySchedulerInfo sinfo = new CapacitySchedulerInfo(root);
					csqinfo.csinfo = sinfo;
					csqinfo.qinfo = null;
					float used = sinfo.GetUsedCapacity() / 100;
					ul.Li().$style("margin-bottom: 1em").Span().$style("font-weight: bold").("Legend:"
						).().Span().$class("qlegend ui-corner-all").$style(QGiven).("Capacity").().Span(
						).$class("qlegend ui-corner-all").$style(QUnder).("Used").().Span().$class("qlegend ui-corner-all"
						).$style(QOver).("Used (over capacity)").().Span().$class("qlegend ui-corner-all ui-state-default"
						).("Max Capacity").().().Li().A(Q).$style(Width(QMaxWidth)).Span().$style(StringHelper.Join
						(Width(used), ";left:0%;", used > 1 ? QOver : QUnder)).(".").().Span(".q", "root"
						).().Span().$class("qstats").$style(Left(QStatsPos)).(StringHelper.Join(Percent(
						used), " used")).().(typeof(CapacitySchedulerPage.QueueBlock)).();
				}
				ul.().().Script().$type("text/javascript").("$('#cs').hide();").().().(typeof(RMAppsBlock
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
				, "  $('#cs').bind('select_node.jstree', function(e, data) {", "    var q = $('.q', data.rslt.obj).first().text();"
				, "    if (q == 'root') q = '';", "    else q = '^' + q.substr(q.lastIndexOf('.') + 1) + '$';"
				, "    $('#apps').dataTable().fnFilter(q, 4, true);", "  });", "  $('#cs').show();"
				, "});").().(typeof(SchedulerPageUtil.QueueBlockUtil));
		}

		// to center info table
		protected override Type Content()
		{
			return typeof(CapacitySchedulerPage.QueuesBlock);
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
