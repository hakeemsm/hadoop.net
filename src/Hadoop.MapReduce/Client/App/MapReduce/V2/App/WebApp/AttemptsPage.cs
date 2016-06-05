using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class AttemptsPage : TaskPage
	{
		internal class FewAttemptsBlock : TaskPage.AttemptsBlock
		{
			[Com.Google.Inject.Inject]
			internal FewAttemptsBlock(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App ctx)
				: base(ctx)
			{
			}

			protected internal override bool IsValidRequest()
			{
				return true;
			}

			protected internal override ICollection<TaskAttempt> GetTaskAttempts()
			{
				IList<TaskAttempt> fewTaskAttemps = new AList<TaskAttempt>();
				string taskTypeStr = $(AMParams.TaskType);
				TaskType taskType = MRApps.TaskType(taskTypeStr);
				string attemptStateStr = $(AMParams.AttemptState);
				MRApps.TaskAttemptStateUI neededState = MRApps.TaskAttemptState(attemptStateStr);
				foreach (Task task in base.app.GetJob().GetTasks(taskType).Values)
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

		protected override Type Content()
		{
			return typeof(AttemptsPage.FewAttemptsBlock);
		}
	}
}
