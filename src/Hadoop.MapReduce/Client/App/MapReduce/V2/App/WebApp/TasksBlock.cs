using System;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class TasksBlock : HtmlBlock
	{
		internal readonly Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app;

		[Com.Google.Inject.Inject]
		internal TasksBlock(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app)
		{
			this.app = app;
		}

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
			Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
				.Table("#tasks").Thead().Tr().Th("Task").Th("Progress").Th("Status").Th("State")
				.Th("Start Time").Th("Finish Time").Th("Elapsed Time").().().Tbody();
			StringBuilder tasksTableData = new StringBuilder("[\n");
			foreach (Task task in app.GetJob().GetTasks().Values)
			{
				if (type != null && task.GetType() != type)
				{
					continue;
				}
				string taskStateStr = $(AMParams.TaskState);
				if (taskStateStr == null || taskStateStr.Trim().Equals(string.Empty))
				{
					taskStateStr = "ALL";
				}
				if (!Sharpen.Runtime.EqualsIgnoreCase(taskStateStr, "ALL"))
				{
					try
					{
						// get stateUI enum
						MRApps.TaskStateUI stateUI = MRApps.TaskState(taskStateStr);
						if (!stateUI.CorrespondsTo(task.GetState()))
						{
							continue;
						}
					}
					catch (ArgumentException)
					{
						continue;
					}
				}
				// not supported state, ignore
				TaskInfo info = new TaskInfo(task);
				string tid = info.GetId();
				string pct = StringHelper.Percent(info.GetProgress() / 100);
				tasksTableData.Append("[\"<a href='").Append(Url("task", tid)).Append("'>").Append
					(tid).Append("</a>\",\"").Append("<br title='").Append(pct).Append("'> <div class='"
					).Append(JQueryUI.CProgressbar).Append("' title='").Append(StringHelper.Join(pct
					, '%')).Append("'> ").Append("<div class='").Append(JQueryUI.CProgressbarValue).
					Append("' style='").Append(StringHelper.Join("width:", pct, '%')).Append("'> </div> </div>\",\""
					).Append(StringEscapeUtils.EscapeJavaScript(StringEscapeUtils.EscapeHtml(info.GetStatus
					()))).Append("\",\"").Append(info.GetState()).Append("\",\"").Append(info.GetStartTime
					()).Append("\",\"").Append(info.GetFinishTime()).Append("\",\"").Append(info.GetElapsedTime
					()).Append("\"],\n");
			}
			//Progress bar
			//Remove the last comma and close off the array of arrays
			if (tasksTableData[tasksTableData.Length - 2] == ',')
			{
				tasksTableData.Delete(tasksTableData.Length - 2, tasksTableData.Length - 1);
			}
			tasksTableData.Append("]");
			html.Script().$type("text/javascript").("var tasksTableData=" + tasksTableData).(
				);
			tbody.().();
		}
	}
}
