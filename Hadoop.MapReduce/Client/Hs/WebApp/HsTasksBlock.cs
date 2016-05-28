using System.Text;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>Render the a table of tasks for a given type.</summary>
	public class HsTasksBlock : HtmlBlock
	{
		internal readonly Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app;

		[Com.Google.Inject.Inject]
		internal HsTasksBlock(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app)
		{
			this.app = app;
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
		*/
		protected override void Render(HtmlBlock.Block html)
		{
			if (app.GetJob() == null)
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
			Hamlet.THEAD<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> thead;
			if (type != null)
			{
				thead = html.Table("#" + app.GetJob().GetID() + type).$class("dt-tasks").Thead();
			}
			else
			{
				thead = html.Table("#tasks").Thead();
			}
			//Create the spanning row
			int attemptColSpan = type == TaskType.Reduce ? 8 : 3;
			thead.Tr().Th().$colspan(5).$class("ui-state-default").("Task").().Th().$colspan(
				attemptColSpan).$class("ui-state-default").("Successful Attempt").().();
			Hamlet.TR<Hamlet.THEAD<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>
				> theadRow = thead.Tr().Th("Name").Th("State").Th("Start Time").Th("Finish Time"
				).Th("Elapsed Time").Th("Start Time");
			//Attempt
			if (type == TaskType.Reduce)
			{
				theadRow.Th("Shuffle Finish Time");
				//Attempt
				theadRow.Th("Merge Finish Time");
			}
			//Attempt
			theadRow.Th("Finish Time");
			//Attempt
			if (type == TaskType.Reduce)
			{
				theadRow.Th("Elapsed Time Shuffle");
				//Attempt
				theadRow.Th("Elapsed Time Merge");
				//Attempt
				theadRow.Th("Elapsed Time Reduce");
			}
			//Attempt
			theadRow.Th("Elapsed Time");
			//Attempt
			Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = theadRow
				.().().Tbody();
			// Write all the data into a JavaScript array of arrays for JQuery
			// DataTables to display
			StringBuilder tasksTableData = new StringBuilder("[\n");
			foreach (Task task in app.GetJob().GetTasks().Values)
			{
				if (type != null && task.GetType() != type)
				{
					continue;
				}
				TaskInfo info = new TaskInfo(task);
				string tid = info.GetId();
				long startTime = info.GetStartTime();
				long finishTime = info.GetFinishTime();
				long elapsed = info.GetElapsedTime();
				long attemptStartTime = -1;
				long shuffleFinishTime = -1;
				long sortFinishTime = -1;
				long attemptFinishTime = -1;
				long elapsedShuffleTime = -1;
				long elapsedSortTime = -1;
				long elapsedReduceTime = -1;
				long attemptElapsed = -1;
				TaskAttempt successful = info.GetSuccessful();
				if (successful != null)
				{
					TaskAttemptInfo ta;
					if (type == TaskType.Reduce)
					{
						ReduceTaskAttemptInfo rta = new ReduceTaskAttemptInfo(successful, type);
						shuffleFinishTime = rta.GetShuffleFinishTime();
						sortFinishTime = rta.GetMergeFinishTime();
						elapsedShuffleTime = rta.GetElapsedShuffleTime();
						elapsedSortTime = rta.GetElapsedMergeTime();
						elapsedReduceTime = rta.GetElapsedReduceTime();
						ta = rta;
					}
					else
					{
						ta = new TaskAttemptInfo(successful, type, false);
					}
					attemptStartTime = ta.GetStartTime();
					attemptFinishTime = ta.GetFinishTime();
					attemptElapsed = ta.GetElapsedTime();
				}
				tasksTableData.Append("[\"").Append("<a href='" + Url("task", tid)).Append("'>").
					Append(tid).Append("</a>\",\"").Append(info.GetState()).Append("\",\"").Append(startTime
					).Append("\",\"").Append(finishTime).Append("\",\"").Append(elapsed).Append("\",\""
					).Append(attemptStartTime).Append("\",\"");
				if (type == TaskType.Reduce)
				{
					tasksTableData.Append(shuffleFinishTime).Append("\",\"").Append(sortFinishTime).Append
						("\",\"");
				}
				tasksTableData.Append(attemptFinishTime).Append("\",\"");
				if (type == TaskType.Reduce)
				{
					tasksTableData.Append(elapsedShuffleTime).Append("\",\"").Append(elapsedSortTime)
						.Append("\",\"").Append(elapsedReduceTime).Append("\",\"");
				}
				tasksTableData.Append(attemptElapsed).Append("\"],\n");
			}
			//Remove the last comma and close off the array of arrays
			if (tasksTableData[tasksTableData.Length - 2] == ',')
			{
				tasksTableData.Delete(tasksTableData.Length - 2, tasksTableData.Length - 1);
			}
			tasksTableData.Append("]");
			html.Script().$type("text/javascript").("var tasksTableData=" + tasksTableData).(
				);
			Hamlet.TR<Hamlet.TFOOT<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>
				> footRow = tbody.().Tfoot().Tr();
			footRow.Th().Input("search_init").$type(HamletSpec.InputType.text).$name("task").
				$value("ID").().().Th().Input("search_init").$type(HamletSpec.InputType.text).$name
				("state").$value("State").().().Th().Input("search_init").$type(HamletSpec.InputType
				.text).$name("start_time").$value("Start Time").().().Th().Input("search_init").
				$type(HamletSpec.InputType.text).$name("finish_time").$value("Finish Time").().(
				).Th().Input("search_init").$type(HamletSpec.InputType.text).$name("elapsed_time"
				).$value("Elapsed Time").().().Th().Input("search_init").$type(HamletSpec.InputType
				.text).$name("attempt_start_time").$value("Start Time").().();
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
				).$value("Elapsed Time").().();
			footRow.().().();
		}
	}
}
