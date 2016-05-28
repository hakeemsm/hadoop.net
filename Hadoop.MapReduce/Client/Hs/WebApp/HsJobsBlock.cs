using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>Render all of the jobs that the history server is aware of.</summary>
	public class HsJobsBlock : HtmlBlock
	{
		internal readonly AppContext appContext;

		internal readonly SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z"
			);

		[Com.Google.Inject.Inject]
		internal HsJobsBlock(AppContext appCtx)
		{
			appContext = appCtx;
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
		*/
		protected override void Render(HtmlBlock.Block html)
		{
			Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
				.H2("Retired Jobs").Table("#jobs").Thead().Tr().Th("Submit Time").Th("Start Time"
				).Th("Finish Time").Th(".id", "Job ID").Th(".name", "Name").Th("User").Th("Queue"
				).Th(".state", "State").Th("Maps Total").Th("Maps Completed").Th("Reduces Total"
				).Th("Reduces Completed").().().Tbody();
			Log.Info("Getting list of all Jobs.");
			// Write all the data into a JavaScript array of arrays for JQuery
			// DataTables to display
			StringBuilder jobsTableData = new StringBuilder("[\n");
			foreach (Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job j in appContext.GetAllJobs().
				Values)
			{
				JobInfo job = new JobInfo(j);
				jobsTableData.Append("[\"").Append(dateFormat.Format(Sharpen.Extensions.CreateDate
					(job.GetSubmitTime()))).Append("\",\"").Append(dateFormat.Format(Sharpen.Extensions.CreateDate
					(job.GetStartTime()))).Append("\",\"").Append(dateFormat.Format(Sharpen.Extensions.CreateDate
					(job.GetFinishTime()))).Append("\",\"").Append("<a href='").Append(Url("job", job
					.GetId())).Append("'>").Append(job.GetId()).Append("</a>\",\"").Append(StringEscapeUtils
					.EscapeJavaScript(StringEscapeUtils.EscapeHtml(job.GetName()))).Append("\",\"").
					Append(StringEscapeUtils.EscapeJavaScript(StringEscapeUtils.EscapeHtml(job.GetUserName
					()))).Append("\",\"").Append(StringEscapeUtils.EscapeJavaScript(StringEscapeUtils
					.EscapeHtml(job.GetQueueName()))).Append("\",\"").Append(job.GetState()).Append(
					"\",\"").Append(job.GetMapsTotal().ToString()).Append("\",\"").Append(job.GetMapsCompleted
					().ToString()).Append("\",\"").Append(job.GetReducesTotal().ToString()).Append("\",\""
					).Append(job.GetReducesCompleted().ToString()).Append("\"],\n");
			}
			//Remove the last comma and close off the array of arrays
			if (jobsTableData[jobsTableData.Length - 2] == ',')
			{
				jobsTableData.Delete(jobsTableData.Length - 2, jobsTableData.Length - 1);
			}
			jobsTableData.Append("]");
			html.Script().$type("text/javascript").("var jobsTableData=" + jobsTableData).();
			tbody.().Tfoot().Tr().Th().Input("search_init").$type(HamletSpec.InputType.text).
				$name("submit_time").$value("Submit Time").().().Th().Input("search_init").$type
				(HamletSpec.InputType.text).$name("start_time").$value("Start Time").().().Th().
				Input("search_init").$type(HamletSpec.InputType.text).$name("finish_time").$value
				("Finish Time").().().Th().Input("search_init").$type(HamletSpec.InputType.text)
				.$name("start_time").$value("Job ID").().().Th().Input("search_init").$type(HamletSpec.InputType
				.text).$name("start_time").$value("Name").().().Th().Input("search_init").$type(
				HamletSpec.InputType.text).$name("start_time").$value("User").().().Th().Input("search_init"
				).$type(HamletSpec.InputType.text).$name("start_time").$value("Queue").().().Th(
				).Input("search_init").$type(HamletSpec.InputType.text).$name("start_time").$value
				("State").().().Th().Input("search_init").$type(HamletSpec.InputType.text).$name
				("start_time").$value("Maps Total").().().Th().Input("search_init").$type(HamletSpec.InputType
				.text).$name("start_time").$value("Maps Completed").().().Th().Input("search_init"
				).$type(HamletSpec.InputType.text).$name("start_time").$value("Reduces Total").(
				).().Th().Input("search_init").$type(HamletSpec.InputType.text).$name("start_time"
				).$value("Reduces Completed").().().().().();
		}
	}
}
