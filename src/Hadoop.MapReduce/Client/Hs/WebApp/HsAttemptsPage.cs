using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>Render a page showing the attempts made of a given type and a given job.
	/// 	</summary>
	public class HsAttemptsPage : HsTaskPage
	{
		internal class FewAttemptsBlock : HsTaskPage.AttemptsBlock
		{
			[Com.Google.Inject.Inject]
			internal FewAttemptsBlock(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App ctx)
				: base(ctx)
			{
			}

			/*
			* (non-Javadoc)
			* @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsTaskPage.AttemptsBlock#isValidRequest()
			* Verify that a job is given.
			*/
			protected internal override bool IsValidRequest()
			{
				return app.GetJob() != null;
			}

			/*
			* (non-Javadoc)
			* @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsTaskPage.AttemptsBlock#getTaskAttempts()
			* @return the attempts that are for a given job and a specific type/state.
			*/
			protected internal override ICollection<TaskAttempt> GetTaskAttempts()
			{
				IList<TaskAttempt> fewTaskAttemps = new AList<TaskAttempt>();
				string taskTypeStr = $(AMParams.TaskType);
				TaskType taskType = MRApps.TaskType(taskTypeStr);
				string attemptStateStr = $(AMParams.AttemptState);
				MRApps.TaskAttemptStateUI neededState = MRApps.TaskAttemptState(attemptStateStr);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job j = app.GetJob();
				IDictionary<TaskId, Task> tasks = j.GetTasks(taskType);
				foreach (Task task in tasks.Values)
				{
					IDictionary<TaskAttemptId, TaskAttempt> attempts = task.GetAttempts();
					foreach (TaskAttempt attempt in attempts.Values)
					{
						if (neededState.CorrespondsTo(attempt.GetState()))
						{
							fewTaskAttemps.AddItem(attempt);
						}
					}
				}
				return fewTaskAttemps;
			}
		}

		/// <summary>The content will render a different set of task attempts.</summary>
		/// <returns>FewAttemptsBlock.class</returns>
		protected override Type Content()
		{
			return typeof(HsAttemptsPage.FewAttemptsBlock);
		}
	}
}
