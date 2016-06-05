using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class JobsBlock : HtmlBlock
	{
		internal readonly AppContext appContext;

		[Com.Google.Inject.Inject]
		internal JobsBlock(AppContext appCtx)
		{
			appContext = appCtx;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
				.H2("Active Jobs").Table("#jobs").Thead().Tr().Th(".id", "Job ID").Th(".name", "Name"
				).Th(".state", "State").Th("Map Progress").Th("Maps Total").Th("Maps Completed")
				.Th("Reduce Progress").Th("Reduces Total").Th("Reduces Completed").().().Tbody();
			foreach (Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job j in appContext.GetAllJobs().
				Values)
			{
				JobInfo job = new JobInfo(j, false);
				tbody.Tr().Td().Span().$title(job.GetId().ToString()).().A(Url("job", job.GetId()
					), job.GetId()).().Td(job.GetName()).Td(job.GetState()).Td().Span().$title(job.GetMapProgressPercent
					()).().Div(JQueryUI.Progressbar).$title(StringHelper.Join(job.GetMapProgressPercent
					(), '%')).Div(JQueryUI.ProgressbarValue).$style(StringHelper.Join("width:", job.
					GetMapProgressPercent(), '%')).().().().Td(job.GetMapsTotal().ToString()).Td(job
					.GetMapsCompleted().ToString()).Td().Span().$title(job.GetReduceProgressPercent(
					)).().Div(JQueryUI.Progressbar).$title(StringHelper.Join(job.GetReduceProgressPercent
					(), '%')).Div(JQueryUI.ProgressbarValue).$style(StringHelper.Join("width:", job.
					GetReduceProgressPercent(), '%')).().().().Td(job.GetReducesTotal().ToString()).
					Td(job.GetReducesCompleted().ToString()).();
			}
			// for sorting
			// for sorting
			// tooltip
			// for sorting
			// tooltip
			tbody.().();
		}
	}
}
