using System;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	internal class DefaultSchedulerPage : RmView
	{
		internal const string Q = ".ui-state-default.ui-corner-all";

		internal const float WidthF = 0.8f;

		internal const string QEnd = "left:101%";

		internal const string Over = "font-size:1px;background:rgba(255, 140, 0, 0.8)";

		internal const string Under = "font-size:1px;background:rgba(50, 205, 50, 0.8)";

		internal const float Epsilon = 1e-8f;

		internal class QueueInfoBlock : HtmlBlock
		{
			internal readonly FifoSchedulerInfo sinfo;

			[Com.Google.Inject.Inject]
			internal QueueInfoBlock(View.ViewContext ctx, ResourceManager rm)
				: base(ctx)
			{
				sinfo = new FifoSchedulerInfo(rm);
			}

			protected override void Render(HtmlBlock.Block html)
			{
				Info("\'" + sinfo.GetQueueName() + "\' Queue Status").("Queue State:", sinfo.GetState
					()).("Minimum Queue Memory Capacity:", Sharpen.Extensions.ToString(sinfo.GetMinQueueMemoryCapacity
					())).("Maximum Queue Memory Capacity:", Sharpen.Extensions.ToString(sinfo.GetMaxQueueMemoryCapacity
					())).("Number of Nodes:", Sharpen.Extensions.ToString(sinfo.GetNumNodes())).("Used Node Capacity:"
					, Sharpen.Extensions.ToString(sinfo.GetUsedNodeCapacity())).("Available Node Capacity:"
					, Sharpen.Extensions.ToString(sinfo.GetAvailNodeCapacity())).("Total Node Capacity:"
					, Sharpen.Extensions.ToString(sinfo.GetTotalNodeCapacity())).("Number of Node Containers:"
					, Sharpen.Extensions.ToString(sinfo.GetNumContainers()));
				html.(typeof(InfoBlock));
			}
		}

		internal class QueuesBlock : HtmlBlock
		{
			internal readonly FifoSchedulerInfo sinfo;

			internal readonly FifoScheduler fs;

			[Com.Google.Inject.Inject]
			internal QueuesBlock(ResourceManager rm)
			{
				sinfo = new FifoSchedulerInfo(rm);
				fs = (FifoScheduler)rm.GetResourceScheduler();
			}

			protected override void Render(HtmlBlock.Block html)
			{
				html.(typeof(MetricsOverviewTable));
				Hamlet.UL<Hamlet.DIV<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>> ul
					 = html.Div("#cs-wrapper.ui-widget").Div(".ui-widget-header.ui-corner-top").("FifoScheduler Queue"
					).().Div("#cs.ui-widget-content.ui-corner-bottom").Ul();
				if (fs == null)
				{
					ul.Li().A(Q).$style(Width(WidthF)).Span().$style(QEnd).("100% ").().Span(".q", "default"
						).().();
				}
				else
				{
					float used = sinfo.GetUsedCapacity();
					float set = sinfo.GetCapacity();
					float delta = Math.Abs(set - used) + 0.001f;
					ul.Li().A(Q).$style(Width(WidthF)).$title(StringHelper.Join("used:", Percent(used
						))).Span().$style(QEnd).("100%").().Span().$style(StringHelper.Join(Width(delta)
						, ';', used > set ? Over : Under, ';', used > set ? Left(set) : Left(used))).("."
						).().Span(".q", sinfo.GetQueueName()).().(typeof(DefaultSchedulerPage.QueueInfoBlock
						)).();
				}
				ul.().().Script().$type("text/javascript").("$('#cs').hide();").().().(typeof(AppsBlock
					));
			}
		}

		protected override void PostHead(Hamlet.HTML<HtmlPage._> html)
		{
			html.Style().$type("text/css").("#cs { padding: 0.5em 0 1em 0; margin-bottom: 1em; position: relative }"
				, "#cs ul { list-style: none }", "#cs a { font-weight: normal; margin: 2px; position: relative }"
				, "#cs a span { font-weight: normal; font-size: 80% }", "#cs-wrapper .ui-widget-header { padding: 0.2em 0.5em }"
				, "table.info tr th {width: 50%}").().Script("/static/jt/jquery.jstree.js").Script
				().$type("text/javascript").("$(function() {", "  $('#cs a span').addClass('ui-corner-all').css('position', 'absolute');"
				, "  $('#cs').bind('loaded.jstree', function (e, data) {", "    data.inst.open_all(); })."
				, "    jstree({", "    core: { animation: 188, html_titles: true },", "    plugins: ['themeroller', 'html_data', 'ui'],"
				, "    themeroller: { item_open: 'ui-icon-minus',", "      item_clsd: 'ui-icon-plus', item_leaf: 'ui-icon-gear'"
				, "    }", "  });", "  $('#cs').bind('select_node.jstree', function(e, data) {", 
				"    var q = $('.q', data.rslt.obj).first().text();", "    if (q == 'root') q = '';"
				, "    $('#apps').dataTable().fnFilter(q, 4);", "  });", "  $('#cs').show();", "});"
				).();
		}

		// to center info table
		protected override Type Content()
		{
			return typeof(DefaultSchedulerPage.QueuesBlock);
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
