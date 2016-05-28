using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class JobBlock : HtmlBlock
	{
		internal readonly AppContext appContext;

		[Com.Google.Inject.Inject]
		internal JobBlock(AppContext appctx)
		{
			appContext = appctx;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			string jid = $(AMParams.JobId);
			if (jid.IsEmpty())
			{
				html.P().("Sorry, can't do anything without a JobID.").();
				return;
			}
			JobId jobID = MRApps.ToJobID(jid);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetJob(jobID);
			if (job == null)
			{
				html.P().("Sorry, ", jid, " not found.").();
				return;
			}
			IList<AMInfo> amInfos = job.GetAMInfos();
			string amString = amInfos.Count == 1 ? "ApplicationMaster" : "ApplicationMasters";
			JobInfo jinfo = new JobInfo(job, true);
			Info("Job Overview").("Job Name:", jinfo.GetName()).("State:", jinfo.GetState()).
				("Uberized:", jinfo.IsUberized()).("Started:", Sharpen.Extensions.CreateDate(jinfo
				.GetStartTime())).("Elapsed:", StringUtils.FormatTime(jinfo.GetElapsedTime()));
			Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> div = html.(typeof(InfoBlock
				)).Div(JQueryUI.InfoWrap);
			// MRAppMasters Table
			Hamlet.TABLE<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> table = div
				.Table("#job");
			table.Tr().Th(amString).().Tr().Th(JQueryUI.Th, "Attempt Number").Th(JQueryUI.Th, 
				"Start Time").Th(JQueryUI.Th, "Node").Th(JQueryUI.Th, "Logs").();
			foreach (AMInfo amInfo in amInfos)
			{
				AMAttemptInfo attempt = new AMAttemptInfo(amInfo, jinfo.GetId(), jinfo.GetUserName
					());
				table.Tr().Td(attempt.GetAttemptId().ToString()).Td(Sharpen.Extensions.CreateDate
					(attempt.GetStartTime()).ToString()).Td().A(".nodelink", Url(MRWebAppUtil.GetYARNWebappScheme
					(), attempt.GetNodeHttpAddress()), attempt.GetNodeHttpAddress()).().Td().A(".logslink"
					, Url(attempt.GetLogsLink()), "logs").().();
			}
			table.();
			div.();
			html.Div(JQueryUI.InfoWrap).Table("#job").Tr().Th(JQueryUI.Th, "Task Type").Th(JQueryUI
				.Th, "Progress").Th(JQueryUI.Th, "Total").Th(JQueryUI.Th, "Pending").Th(JQueryUI
				.Th, "Running").Th(JQueryUI.Th, "Complete").().Tr(JQueryUI.Odd).Th("Map").Td().Div
				(JQueryUI.Progressbar).$title(StringHelper.Join(jinfo.GetMapProgressPercent(), '%'
				)).Div(JQueryUI.ProgressbarValue).$style(StringHelper.Join("width:", jinfo.GetMapProgressPercent
				(), '%')).().().().Td().A(Url("tasks", jid, "m", "ALL"), jinfo.GetMapsTotal().ToString
				()).().Td().A(Url("tasks", jid, "m", "PENDING"), jinfo.GetMapsPending().ToString
				()).().Td().A(Url("tasks", jid, "m", "RUNNING"), jinfo.GetMapsRunning().ToString
				()).().Td().A(Url("tasks", jid, "m", "COMPLETED"), jinfo.GetMapsCompleted().ToString
				()).().().Tr(JQueryUI.Even).Th("Reduce").Td().Div(JQueryUI.Progressbar).$title(StringHelper.Join
				(jinfo.GetReduceProgressPercent(), '%')).Div(JQueryUI.ProgressbarValue).$style(StringHelper.Join
				("width:", jinfo.GetReduceProgressPercent(), '%')).().().().Td().A(Url("tasks", 
				jid, "r", "ALL"), jinfo.GetReducesTotal().ToString()).().Td().A(Url("tasks", jid
				, "r", "PENDING"), jinfo.GetReducesPending().ToString()).().Td().A(Url("tasks", 
				jid, "r", "RUNNING"), jinfo.GetReducesRunning().ToString()).().Td().A(Url("tasks"
				, jid, "r", "COMPLETED"), jinfo.GetReducesCompleted().ToString()).().().().Table
				("#job").Tr().Th(JQueryUI.Th, "Attempt Type").Th(JQueryUI.Th, "New").Th(JQueryUI
				.Th, "Running").Th(JQueryUI.Th, "Failed").Th(JQueryUI.Th, "Killed").Th(JQueryUI.
				Th, "Successful").().Tr(JQueryUI.Odd).Th("Maps").Td().A(Url("attempts", jid, "m"
				, MRApps.TaskAttemptStateUI.New.ToString()), jinfo.GetNewMapAttempts().ToString(
				)).().Td().A(Url("attempts", jid, "m", MRApps.TaskAttemptStateUI.Running.ToString
				()), jinfo.GetRunningMapAttempts().ToString()).().Td().A(Url("attempts", jid, "m"
				, MRApps.TaskAttemptStateUI.Failed.ToString()), jinfo.GetFailedMapAttempts().ToString
				()).().Td().A(Url("attempts", jid, "m", MRApps.TaskAttemptStateUI.Killed.ToString
				()), jinfo.GetKilledMapAttempts().ToString()).().Td().A(Url("attempts", jid, "m"
				, MRApps.TaskAttemptStateUI.Successful.ToString()), jinfo.GetSuccessfulMapAttempts
				().ToString()).().().Tr(JQueryUI.Even).Th("Reduces").Td().A(Url("attempts", jid, 
				"r", MRApps.TaskAttemptStateUI.New.ToString()), jinfo.GetNewReduceAttempts().ToString
				()).().Td().A(Url("attempts", jid, "r", MRApps.TaskAttemptStateUI.Running.ToString
				()), jinfo.GetRunningReduceAttempts().ToString()).().Td().A(Url("attempts", jid, 
				"r", MRApps.TaskAttemptStateUI.Failed.ToString()), jinfo.GetFailedReduceAttempts
				().ToString()).().Td().A(Url("attempts", jid, "r", MRApps.TaskAttemptStateUI.Killed
				.ToString()), jinfo.GetKilledReduceAttempts().ToString()).().Td().A(Url("attempts"
				, jid, "r", MRApps.TaskAttemptStateUI.Successful.ToString()), jinfo.GetSuccessfulReduceAttempts
				().ToString()).().().().();
		}
		// Tasks table
		// tooltip
		// tooltip
		// Attempts table
	}
}
