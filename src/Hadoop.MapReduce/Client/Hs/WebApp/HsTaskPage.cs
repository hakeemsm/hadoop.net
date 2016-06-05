using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>A Page the shows the status of a given task</summary>
	public class HsTaskPage : HsView
	{
		/// <summary>A Block of HTML that will render a given task attempt.</summary>
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
				TaskType type = null;
				string symbol = $(AMParams.TaskType);
				if (!symbol.IsEmpty())
				{
					type = MRApps.TaskType(symbol);
				}
				else
				{
					type = app.GetTask().GetType();
				}
				Hamlet.TR<Hamlet.THEAD<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>
					> headRow = html.Table("#attempts").Thead().Tr();
				headRow.Th(".id", "Attempt").Th(".state", "State").Th(".status", "Status").Th(".node"
					, "Node").Th(".logs", "Logs").Th(".tsh", "Start Time");
				if (type == TaskType.Reduce)
				{
					headRow.Th("Shuffle Finish Time");
					headRow.Th("Merge Finish Time");
				}
				headRow.Th("Finish Time");
				//Attempt
				if (type == TaskType.Reduce)
				{
					headRow.Th("Elapsed Time Shuffle");
					//Attempt
					headRow.Th("Elapsed Time Merge");
					//Attempt
					headRow.Th("Elapsed Time Reduce");
				}
				//Attempt
				headRow.Th("Elapsed Time").Th(".note", "Note");
				Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = headRow
					.().().Tbody();
				// Write all the data into a JavaScript array of arrays for JQuery
				// DataTables to display
				StringBuilder attemptsTableData = new StringBuilder("[\n");
				foreach (TaskAttempt attempt in GetTaskAttempts())
				{
					TaskAttemptInfo ta = new TaskAttemptInfo(attempt, false);
					string taid = ta.GetId();
					string nodeHttpAddr = ta.GetNode();
					string containerIdString = ta.GetAssignedContainerIdStr();
					string nodeIdString = attempt.GetAssignedContainerMgrAddress();
					string nodeRackName = ta.GetRack();
					long attemptStartTime = ta.GetStartTime();
					long shuffleFinishTime = -1;
					long sortFinishTime = -1;
					long attemptFinishTime = ta.GetFinishTime();
					long elapsedShuffleTime = -1;
					long elapsedSortTime = -1;
					long elapsedReduceTime = -1;
					if (type == TaskType.Reduce)
					{
						shuffleFinishTime = attempt.GetShuffleFinishTime();
						sortFinishTime = attempt.GetSortFinishTime();
						elapsedShuffleTime = Times.Elapsed(attemptStartTime, shuffleFinishTime, false);
						elapsedSortTime = Times.Elapsed(shuffleFinishTime, sortFinishTime, false);
						elapsedReduceTime = Times.Elapsed(sortFinishTime, attemptFinishTime, false);
					}
					long attemptElapsed = Times.Elapsed(attemptStartTime, attemptFinishTime, false);
					int sortId = attempt.GetID().GetId() + (attempt.GetID().GetTaskId().GetId() * 10000
						);
					attemptsTableData.Append("[\"").Append(sortId + " ").Append(taid).Append("\",\"")
						.Append(ta.GetState()).Append("\",\"").Append(StringEscapeUtils.EscapeJavaScript
						(StringEscapeUtils.EscapeHtml(ta.GetStatus()))).Append("\",\"").Append("<a class='nodelink' href='"
						 + MRWebAppUtil.GetYARNWebappScheme() + nodeHttpAddr + "'>").Append(nodeRackName
						 + "/" + nodeHttpAddr + "</a>\",\"").Append("<a class='logslink' href='").Append
						(Url("logs", nodeIdString, containerIdString, taid, app.GetJob().GetUserName()))
						.Append("'>logs</a>\",\"").Append(attemptStartTime).Append("\",\"");
					if (type == TaskType.Reduce)
					{
						attemptsTableData.Append(shuffleFinishTime).Append("\",\"").Append(sortFinishTime
							).Append("\",\"");
					}
					attemptsTableData.Append(attemptFinishTime).Append("\",\"");
					if (type == TaskType.Reduce)
					{
						attemptsTableData.Append(elapsedShuffleTime).Append("\",\"").Append(elapsedSortTime
							).Append("\",\"").Append(elapsedReduceTime).Append("\",\"");
					}
					attemptsTableData.Append(attemptElapsed).Append("\",\"").Append(StringEscapeUtils
						.EscapeJavaScript(StringEscapeUtils.EscapeHtml(ta.GetNote()))).Append("\"],\n");
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
				Hamlet.TR<Hamlet.TFOOT<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>
					> footRow = tbody.().Tfoot().Tr();
				footRow.Th().Input("search_init").$type(HamletSpec.InputType.text).$name("attempt_name"
					).$value("Attempt").().().Th().Input("search_init").$type(HamletSpec.InputType.text
					).$name("attempt_state").$value("State").().().Th().Input("search_init").$type(HamletSpec.InputType
					.text).$name("attempt_status").$value("Status").().().Th().Input("search_init").
					$type(HamletSpec.InputType.text).$name("attempt_node").$value("Node").().().Th()
					.Input("search_init").$type(HamletSpec.InputType.text).$name("attempt_node").$value
					("Logs").().().Th().Input("search_init").$type(HamletSpec.InputType.text).$name(
					"attempt_start_time").$value("Start Time").().();
				if (type == TaskType.Reduce)
				{
					footRow.Th().Input("search_init").$type(HamletSpec.InputType.text).$name("shuffle_time"
						).$value("Shuffle Time").().();
					footRow.Th().Input("search_init").$type(HamletSpec.InputType.text).$name("merge_time"
						).$value("Merge Time").().();
				}
				footRow.Th().Input("search_init").$type(HamletSpec.InputType.text).$name("attempt_finish"
					).$value("Finish Time").().();
				if (type == TaskType.Reduce)
				{
					footRow.Th().Input("search_init").$type(HamletSpec.InputType.text).$name("elapsed_shuffle_time"
						).$value("Elapsed Shuffle Time").().();
					footRow.Th().Input("search_init").$type(HamletSpec.InputType.text).$name("elapsed_merge_time"
						).$value("Elapsed Merge Time").().();
					footRow.Th().Input("search_init").$type(HamletSpec.InputType.text).$name("elapsed_reduce_time"
						).$value("Elapsed Reduce Time").().();
				}
				footRow.Th().Input("search_init").$type(HamletSpec.InputType.text).$name("attempt_elapsed"
					).$value("Elapsed Time").().().Th().Input("search_init").$type(HamletSpec.InputType
					.text).$name("note").$value("Note").().();
				footRow.().().();
			}

			/// <returns>true if this is a valid request else false.</returns>
			protected internal virtual bool IsValidRequest()
			{
				return app.GetTask() != null;
			}

			/// <returns>all of the attempts to render.</returns>
			protected internal virtual ICollection<TaskAttempt> GetTaskAttempts()
			{
				return app.GetTask().GetAttempts().Values;
			}
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
		*/
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			//override the nav config from commonPReHead
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:2}");
			//Set up the java script and CSS for the attempts table
			Set(JQueryUI.DatatablesId, "attempts");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "attempts"), AttemptsTableInit());
			Set(JQueryUI.PostInitID(JQueryUI.Datatables, "attempts"), AttemptsPostTableInit()
				);
			SetTableStyles(html, "attempts");
		}

		/// <summary>The content of this page is the attempts block</summary>
		/// <returns>AttemptsBlock.class</returns>
		protected override Type Content()
		{
			return typeof(HsTaskPage.AttemptsBlock);
		}

		/// <returns>
		/// The end of the JS map that is the jquery datatable config for the
		/// attempts table.
		/// </returns>
		private string AttemptsTableInit()
		{
			TaskType type = null;
			string symbol = $(AMParams.TaskType);
			if (!symbol.IsEmpty())
			{
				type = MRApps.TaskType(symbol);
			}
			else
			{
				TaskId taskID = MRApps.ToTaskID($(AMParams.TaskId));
				type = taskID.GetTaskType();
			}
			StringBuilder b = JQueryUI.TableInit().Append(", 'aaData': attemptsTableData").Append
				(", bDeferRender: true").Append(", bProcessing: true").Append("\n,aoColumnDefs:[\n"
				).Append("\n{'aTargets': [ 4 ]").Append(", 'bSearchable': false }").Append("\n, {'sType':'numeric', 'aTargets': [ 0 ]"
				).Append(", 'mRender': parseHadoopAttemptID }").Append("\n, {'sType':'numeric', 'aTargets': [ 5, 6"
				).Append(type == TaskType.Reduce ? ", 7, 8" : string.Empty).Append(" ], 'mRender': renderHadoopDate }"
				).Append("\n, {'sType':'numeric', 'aTargets': [").Append(type == TaskType.Reduce
				 ? "9, 10, 11, 12" : "7").Append(" ], 'mRender': renderHadoopElapsedTime }]").Append
				("\n, aaSorting: [[0, 'asc']]").Append("}");
			//logs column should not filterable (it includes container ID which may pollute searches)
			//Column numbers are different for maps and reduces
			// Sort by id upon page load
			return b.ToString();
		}

		private string AttemptsPostTableInit()
		{
			return "var asInitVals = new Array();\n" + "$('tfoot input').keyup( function () \n{"
				 + "  attemptsDataTable.fnFilter( this.value, $('tfoot input').index(this) );\n"
				 + "} );\n" + "$('tfoot input').each( function (i) {\n" + "  asInitVals[i] = this.value;\n"
				 + "} );\n" + "$('tfoot input').focus( function () {\n" + "  if ( this.className == 'search_init' )\n"
				 + "  {\n" + "    this.className = '';\n" + "    this.value = '';\n" + "  }\n" +
				 "} );\n" + "$('tfoot input').blur( function (i) {\n" + "  if ( this.value == '' )\n"
				 + "  {\n" + "    this.className = 'search_init';\n" + "    this.value = asInitVals[$('tfoot input').index(this)];\n"
				 + "  }\n" + "} );\n";
		}
	}
}
