using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class SingleCounterBlock : HtmlBlock
	{
		protected internal SortedDictionary<string, long> values = new SortedDictionary<string
			, long>();

		protected internal Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job;

		protected internal Task task;

		[Com.Google.Inject.Inject]
		internal SingleCounterBlock(AppContext appCtx, View.ViewContext ctx)
			: base(ctx)
		{
			this.PopulateMembers(appCtx);
		}

		protected override void Render(HtmlBlock.Block html)
		{
			if (job == null)
			{
				html.P().("Sorry, no counters for nonexistent", $(AMParams.JobId, "job")).();
				return;
			}
			if (!$(AMParams.TaskId).IsEmpty() && task == null)
			{
				html.P().("Sorry, no counters for nonexistent", $(AMParams.TaskId, "task")).();
				return;
			}
			string columnType = task == null ? "Task" : "Task Attempt";
			Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>
				>> tbody = html.Div(JQueryUI.InfoWrap).Table("#singleCounter").Thead().Tr().Th(".ui-state-default"
				, columnType).Th(".ui-state-default", "Value").().().Tbody();
			foreach (KeyValuePair<string, long> entry in values)
			{
				Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet
					>>>> row = tbody.Tr();
				string id = entry.Key;
				string val = entry.Value.ToString();
				if (task != null)
				{
					row.Td(id);
					row.Td().Br().$title(val).().(val).();
				}
				else
				{
					row.Td().A(Url("singletaskcounter", entry.Key, $(AMParams.CounterGroup), $(AMParams
						.CounterName)), id).();
					row.Td().Br().$title(val).().A(Url("singletaskcounter", entry.Key, $(AMParams.CounterGroup
						), $(AMParams.CounterName)), val).();
				}
				row.();
			}
			tbody.().().();
		}

		private void PopulateMembers(AppContext ctx)
		{
			JobId jobID = null;
			TaskId taskID = null;
			string tid = $(AMParams.TaskId);
			if (!tid.IsEmpty())
			{
				taskID = MRApps.ToTaskID(tid);
				jobID = taskID.GetJobId();
			}
			else
			{
				string jid = $(AMParams.JobId);
				if (!jid.IsEmpty())
				{
					jobID = MRApps.ToJobID(jid);
				}
			}
			if (jobID == null)
			{
				return;
			}
			job = ctx.GetJob(jobID);
			if (job == null)
			{
				return;
			}
			if (taskID != null)
			{
				task = job.GetTask(taskID);
				if (task == null)
				{
					return;
				}
				foreach (KeyValuePair<TaskAttemptId, TaskAttempt> entry in task.GetAttempts())
				{
					long value = 0;
					Counters counters = entry.Value.GetCounters();
					CounterGroup group = (counters != null) ? counters.GetGroup($(AMParams.CounterGroup
						)) : null;
					if (group != null)
					{
						Counter c = group.FindCounter($(AMParams.CounterName));
						if (c != null)
						{
							value = c.GetValue();
						}
					}
					values[MRApps.ToString(entry.Key)] = value;
				}
				return;
			}
			// Get all types of counters
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			foreach (KeyValuePair<TaskId, Task> entry_1 in tasks)
			{
				long value = 0;
				Counters counters = entry_1.Value.GetCounters();
				CounterGroup group = (counters != null) ? counters.GetGroup($(AMParams.CounterGroup
					)) : null;
				if (group != null)
				{
					Counter c = group.FindCounter($(AMParams.CounterName));
					if (c != null)
					{
						value = c.GetValue();
					}
				}
				values[MRApps.ToString(entry_1.Key)] = value;
			}
		}
	}
}
