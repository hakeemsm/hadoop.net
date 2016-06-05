using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class TaskPage : AppView
	{
		internal class AttemptsBlock : HtmlBlock
		{
			internal readonly Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app;

			[Com.Google.Inject.Inject]
			internal AttemptsBlock(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App ctx)
			{
				app = ctx;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				if (!IsValidRequest())
				{
					html.H2($(Title));
					return;
				}
				Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
					.Table("#attempts").Thead().Tr().Th(".id", "Attempt").Th(".progress", "Progress"
					).Th(".state", "State").Th(".status", "Status").Th(".node", "Node").Th(".logs", 
					"Logs").Th(".tsh", "Started").Th(".tsh", "Finished").Th(".tsh", "Elapsed").Th(".note"
					, "Note").().().Tbody();
				// Write all the data into a JavaScript array of arrays for JQuery
				// DataTables to display
				StringBuilder attemptsTableData = new StringBuilder("[\n");
				foreach (TaskAttempt attempt in GetTaskAttempts())
				{
					TaskAttemptInfo ta = new TaskAttemptInfo(attempt, true);
					string progress = StringHelper.Percent(ta.GetProgress() / 100);
					string nodeHttpAddr = ta.GetNode();
					string diag = ta.GetNote() == null ? string.Empty : ta.GetNote();
					attemptsTableData.Append("[\"").Append(ta.GetId()).Append("\",\"").Append(progress
						).Append("\",\"").Append(ta.GetState().ToString()).Append("\",\"").Append(StringEscapeUtils
						.EscapeJavaScript(StringEscapeUtils.EscapeHtml(ta.GetStatus()))).Append("\",\"")
						.Append(nodeHttpAddr == null ? "N/A" : "<a class='nodelink' href='" + MRWebAppUtil
						.GetYARNWebappScheme() + nodeHttpAddr + "'>" + nodeHttpAddr + "</a>").Append("\",\""
						).Append(ta.GetAssignedContainerId() == null ? "N/A" : "<a class='logslink' href='"
						 + Url(MRWebAppUtil.GetYARNWebappScheme(), nodeHttpAddr, "node", "containerlogs"
						, ta.GetAssignedContainerIdStr(), app.GetJob().GetUserName()) + "'>logs</a>").Append
						("\",\"").Append(ta.GetStartTime()).Append("\",\"").Append(ta.GetFinishTime()).Append
						("\",\"").Append(ta.GetElapsedTime()).Append("\",\"").Append(StringEscapeUtils.EscapeJavaScript
						(StringEscapeUtils.EscapeHtml(diag))).Append("\"],\n");
				}
				//Remove the last comma and close off the array of arrays
				if (attemptsTableData[attemptsTableData.Length - 2] == ',')
				{
					attemptsTableData.Delete(attemptsTableData.Length - 2, attemptsTableData.Length -
						 1);
				}
				attemptsTableData.Append("]");
				html.Script().$type("text/javascript").("var attemptsTableData=" + attemptsTableData
					).();
				tbody.().();
			}

			protected internal virtual bool IsValidRequest()
			{
				return app.GetTask() != null;
			}

			protected internal virtual ICollection<TaskAttempt> GetTaskAttempts()
			{
				return app.GetTask().GetAttempts().Values;
			}
		}

		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:3}");
			Set(JQueryUI.DatatablesId, "attempts");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "attempts"), AttemptsTableInit());
			SetTableStyles(html, "attempts");
		}

		protected override Type Content()
		{
			return typeof(TaskPage.AttemptsBlock);
		}

		private string AttemptsTableInit()
		{
			return JQueryUI.TableInit().Append(", 'aaData': attemptsTableData").Append(", bDeferRender: true"
				).Append(", bProcessing: true").Append("\n,aoColumnDefs:[\n").Append("\n{'aTargets': [ 5 ]"
				).Append(", 'bSearchable': false }").Append("\n, {'sType':'numeric', 'aTargets': [ 6, 7"
				).Append(" ], 'mRender': renderHadoopDate }").Append("\n, {'sType':'numeric', 'aTargets': [ 8"
				).Append(" ], 'mRender': renderHadoopElapsedTime }]").Append("\n, aaSorting: [[0, 'asc']]"
				).Append("}").ToString();
		}
		//logs column should not filterable (it includes container ID which may pollute searches)
		// Sort by id upon page load
	}
}
