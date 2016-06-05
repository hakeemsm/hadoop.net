using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>Render a block of HTML for a give job.</summary>
	public class HsJobBlock : HtmlBlock
	{
		internal readonly AppContext appContext;

		[Com.Google.Inject.Inject]
		internal HsJobBlock(AppContext appctx)
		{
			appContext = appctx;
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
		*/
		protected override void Render(HtmlBlock.Block html)
		{
			string jid = $(AMParams.JobId);
			if (jid.IsEmpty())
			{
				html.P().("Sorry, can't do anything without a JobID.").();
				return;
			}
			JobId jobID = MRApps.ToJobID(jid);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job j = appContext.GetJob(jobID);
			if (j == null)
			{
				html.P().("Sorry, ", jid, " not found.").();
				return;
			}
			IList<AMInfo> amInfos = j.GetAMInfos();
			JobInfo job = new JobInfo(j);
			ResponseInfo infoBlock = Info("Job Overview").("Job Name:", job.GetName()).("User Name:"
				, job.GetUserName()).("Queue:", job.GetQueueName()).("State:", job.GetState()).(
				"Uberized:", job.IsUber()).("Submitted:", Sharpen.Extensions.CreateDate(job.GetSubmitTime
				())).("Started:", Sharpen.Extensions.CreateDate(job.GetStartTime())).("Finished:"
				, Sharpen.Extensions.CreateDate(job.GetFinishTime())).("Elapsed:", StringUtils.FormatTime
				(Times.Elapsed(job.GetStartTime(), job.GetFinishTime(), false)));
			string amString = amInfos.Count == 1 ? "ApplicationMaster" : "ApplicationMasters";
			// todo - switch to use JobInfo
			IList<string> diagnostics = j.GetDiagnostics();
			if (diagnostics != null && !diagnostics.IsEmpty())
			{
				StringBuilder b = new StringBuilder();
				foreach (string diag in diagnostics)
				{
					b.Append(diag);
				}
				infoBlock.("Diagnostics:", b.ToString());
			}
			if (job.GetNumMaps() > 0)
			{
				infoBlock.("Average Map Time", StringUtils.FormatTime(job.GetAvgMapTime()));
			}
			if (job.GetNumReduces() > 0)
			{
				infoBlock.("Average Shuffle Time", StringUtils.FormatTime(job.GetAvgShuffleTime()
					));
				infoBlock.("Average Merge Time", StringUtils.FormatTime(job.GetAvgMergeTime()));
				infoBlock.("Average Reduce Time", StringUtils.FormatTime(job.GetAvgReduceTime()));
			}
			foreach (ConfEntryInfo entry in job.GetAcls())
			{
				infoBlock.("ACL " + entry.GetName() + ":", entry.GetValue());
			}
			Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> div = html.(typeof(InfoBlock
				)).Div(JQueryUI.InfoWrap);
			// MRAppMasters Table
			Hamlet.TABLE<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> table = div
				.Table("#job");
			table.Tr().Th(amString).().Tr().Th(JQueryUI.Th, "Attempt Number").Th(JQueryUI.Th, 
				"Start Time").Th(JQueryUI.Th, "Node").Th(JQueryUI.Th, "Logs").();
			bool odd = false;
			foreach (AMInfo amInfo in amInfos)
			{
				AMAttemptInfo attempt = new AMAttemptInfo(amInfo, job.GetId(), job.GetUserName(), 
					string.Empty, string.Empty);
				table.Tr((odd = !odd) ? JQueryUI.Odd : JQueryUI.Even).Td(attempt.GetAttemptId().ToString
					()).Td(Sharpen.Extensions.CreateDate(attempt.GetStartTime()).ToString()).Td().A(
					".nodelink", Url(MRWebAppUtil.GetYARNWebappScheme(), attempt.GetNodeHttpAddress(
					)), attempt.GetNodeHttpAddress()).().Td().A(".logslink", Url(attempt.GetShortLogsLink
					()), "logs").().();
			}
			table.();
			div.();
			html.Div(JQueryUI.InfoWrap).Table("#job").Tr().Th(JQueryUI.Th, "Task Type").Th(JQueryUI
				.Th, "Total").Th(JQueryUI.Th, "Complete").().Tr(JQueryUI.Odd).Th().A(Url("tasks"
				, jid, "m"), "Map").().Td(job.GetMapsTotal().ToString().ToString()).Td(job.GetMapsCompleted
				().ToString().ToString()).().Tr(JQueryUI.Even).Th().A(Url("tasks", jid, "r"), "Reduce"
				).().Td(job.GetReducesTotal().ToString().ToString()).Td(job.GetReducesCompleted(
				).ToString().ToString()).().().Table("#job").Tr().Th(JQueryUI.Th, "Attempt Type"
				).Th(JQueryUI.Th, "Failed").Th(JQueryUI.Th, "Killed").Th(JQueryUI.Th, "Successful"
				).().Tr(JQueryUI.Odd).Th("Maps").Td().A(Url("attempts", jid, "m", MRApps.TaskAttemptStateUI
				.Failed.ToString()), job.GetFailedMapAttempts().ToString()).().Td().A(Url("attempts"
				, jid, "m", MRApps.TaskAttemptStateUI.Killed.ToString()), job.GetKilledMapAttempts
				().ToString()).().Td().A(Url("attempts", jid, "m", MRApps.TaskAttemptStateUI.Successful
				.ToString()), job.GetSuccessfulMapAttempts().ToString()).().().Tr(JQueryUI.Even)
				.Th("Reduces").Td().A(Url("attempts", jid, "r", MRApps.TaskAttemptStateUI.Failed
				.ToString()), job.GetFailedReduceAttempts().ToString()).().Td().A(Url("attempts"
				, jid, "r", MRApps.TaskAttemptStateUI.Killed.ToString()), job.GetKilledReduceAttempts
				().ToString()).().Td().A(Url("attempts", jid, "r", MRApps.TaskAttemptStateUI.Successful
				.ToString()), job.GetSuccessfulReduceAttempts().ToString()).().().().();
		}
		// Tasks table
		// Attempts table
	}
}
