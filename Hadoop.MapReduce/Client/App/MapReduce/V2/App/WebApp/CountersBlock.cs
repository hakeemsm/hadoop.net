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
	public class CountersBlock : HtmlBlock
	{
		internal Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job;

		internal Task task;

		internal Counters total;

		internal Counters map;

		internal Counters reduce;

		[Com.Google.Inject.Inject]
		internal CountersBlock(AppContext appCtx, View.ViewContext ctx)
			: base(ctx)
		{
			GetCounters(appCtx);
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
			if (total == null || total.GetGroupNames() == null || total.CountCounters() == 0)
			{
				string type = $(AMParams.TaskId);
				if (type == null || type.IsEmpty())
				{
					type = $(AMParams.JobId, "the job");
				}
				html.P().("Sorry it looks like ", type, " has no counters.").();
				return;
			}
			string urlBase;
			string urlId;
			if (task != null)
			{
				urlBase = "singletaskcounter";
				urlId = MRApps.ToString(task.GetID());
			}
			else
			{
				urlBase = "singlejobcounter";
				urlId = MRApps.ToString(job.GetID());
			}
			int numGroups = 0;
			Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>
				>> tbody = html.Div(JQueryUI.InfoWrap).Table("#counters").Thead().Tr().Th(".group.ui-state-default"
				, "Counter Group").Th(".ui-state-default", "Counters").().().Tbody();
			foreach (CounterGroup g in total)
			{
				CounterGroup mg = map == null ? null : map.GetGroup(g.GetName());
				CounterGroup rg = reduce == null ? null : reduce.GetGroup(g.GetName());
				++numGroups;
				// This is mostly for demonstration :) Typically we'd introduced
				// a CounterGroup block to reduce the verbosity. OTOH, this
				// serves as an indicator of where we're in the tag hierarchy.
				Hamlet.TR<Hamlet.THEAD<Hamlet.TABLE<Hamlet.TD<Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE
					<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>>>>>>> groupHeadRow = tbody
					.Tr().Th().$title(g.GetName()).$class("ui-state-default").(FixGroupDisplayName(g
					.GetDisplayName())).().Td().$class(JQueryUI.CTable).Table(".dt-counters").$id(job
					.GetID() + "." + g.GetName()).Thead().Tr().Th(".name", "Name");
				if (map != null)
				{
					groupHeadRow.Th("Map").Th("Reduce");
				}
				// Ditto
				Hamlet.TBODY<Hamlet.TABLE<Hamlet.TD<Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV
					<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>>>>>> group = groupHeadRow.Th(map 
					== null ? "Value" : "Total").().().Tbody();
				foreach (Counter counter in g)
				{
					// Ditto
					Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet.TD<Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE
						<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>>>>>>> groupRow = group
						.Tr();
					if (task == null && mg == null && rg == null)
					{
						groupRow.Td().$title(counter.GetName()).(counter.GetDisplayName()).();
					}
					else
					{
						groupRow.Td().$title(counter.GetName()).A(Url(urlBase, urlId, g.GetName(), counter
							.GetName()), counter.GetDisplayName()).();
					}
					if (map != null)
					{
						Counter mc = mg == null ? null : mg.FindCounter(counter.GetName());
						Counter rc = rg == null ? null : rg.FindCounter(counter.GetName());
						groupRow.Td(mc == null ? "0" : string.Format("%,d", mc.GetValue())).Td(rc == null
							 ? "0" : string.Format("%,d", rc.GetValue()));
					}
					groupRow.Td(string.Format("%,d", counter.GetValue())).();
				}
				group.().().().();
			}
			tbody.().().();
		}

		private void GetCounters(AppContext ctx)
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
				if (jid != null && !jid.IsEmpty())
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
				total = task.GetCounters();
				return;
			}
			// Get all types of counters
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			total = job.GetAllCounters();
			bool needTotalCounters = false;
			if (total == null)
			{
				total = new Counters();
				needTotalCounters = true;
			}
			map = new Counters();
			reduce = new Counters();
			foreach (Task t in tasks.Values)
			{
				Counters counters = t.GetCounters();
				if (counters == null)
				{
					continue;
				}
				switch (t.GetType())
				{
					case TaskType.Map:
					{
						map.IncrAllCounters(counters);
						break;
					}

					case TaskType.Reduce:
					{
						reduce.IncrAllCounters(counters);
						break;
					}
				}
				if (needTotalCounters)
				{
					total.IncrAllCounters(counters);
				}
			}
		}

		private string FixGroupDisplayName(CharSequence name)
		{
			return name.ToString().Replace(".", ".\u200B").Replace("$", "\u200B$");
		}
	}
}
