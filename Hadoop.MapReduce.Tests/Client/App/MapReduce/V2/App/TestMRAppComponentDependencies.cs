using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class TestMRAppComponentDependencies
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestComponentStopOrder()
		{
			TestMRAppComponentDependencies.TestMRApp app = new TestMRAppComponentDependencies.TestMRApp
				(this, 1, 1, true, this.GetType().FullName, true);
			JobImpl job = (JobImpl)app.Submit(new Configuration());
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			int waitTime = 20 * 1000;
			while (waitTime > 0 && app.numStops < 2)
			{
				Sharpen.Thread.Sleep(100);
				waitTime -= 100;
			}
			// assert JobHistoryEventHandlerStopped and then clientServiceStopped
			NUnit.Framework.Assert.AreEqual(1, app.JobHistoryEventHandlerStopped);
			NUnit.Framework.Assert.AreEqual(2, app.clientServiceStopped);
		}

		private sealed class TestMRApp : MRApp
		{
			internal int JobHistoryEventHandlerStopped;

			internal int clientServiceStopped;

			internal int numStops;

			public TestMRApp(TestMRAppComponentDependencies _enclosing, int maps, int reduces
				, bool autoComplete, string testName, bool cleanOnStart)
				: base(maps, reduces, autoComplete, testName, cleanOnStart)
			{
				this._enclosing = _enclosing;
				this.JobHistoryEventHandlerStopped = 0;
				this.clientServiceStopped = 0;
				this.numStops = 0;
			}

			protected internal override Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job CreateJob(
				Configuration conf, JobStateInternal forcedState, string diagnostic)
			{
				UserGroupInformation currentUser = null;
				try
				{
					currentUser = UserGroupInformation.GetCurrentUser();
				}
				catch (IOException e)
				{
					throw new YarnRuntimeException(e);
				}
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job newJob = new MRApp.TestJob(this, this.
					GetJobId(), this.GetAttemptID(), conf, this.GetDispatcher().GetEventHandler(), this
					.GetTaskAttemptListener(), this.GetContext().GetClock(), this.GetCommitter(), this
					.IsNewApiCommitter(), currentUser.GetUserName(), this.GetContext(), forcedState, 
					diagnostic);
				((AppContext)this.GetContext()).GetAllJobs()[newJob.GetID()] = newJob;
				this.GetDispatcher().Register(typeof(JobFinishEvent.Type), this.CreateJobFinishEventHandler
					());
				return newJob;
			}

			protected internal override ClientService CreateClientService(AppContext context)
			{
				return new _MRClientService_98(this, context);
			}

			private sealed class _MRClientService_98 : MRClientService
			{
				public _MRClientService_98(TestMRApp _enclosing, AppContext baseArg1)
					: base(baseArg1)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				protected override void ServiceStop()
				{
					this._enclosing.numStops++;
					this._enclosing.clientServiceStopped = this._enclosing.numStops;
					base.ServiceStop();
				}

				private readonly TestMRApp _enclosing;
			}

			protected internal override EventHandler<JobHistoryEvent> CreateJobHistoryHandler
				(AppContext context)
			{
				return new _JobHistoryEventHandler_111(this, context, this.GetStartCount());
			}

			private sealed class _JobHistoryEventHandler_111 : JobHistoryEventHandler
			{
				public _JobHistoryEventHandler_111(TestMRApp _enclosing, AppContext baseArg1, int
					 baseArg2)
					: base(baseArg1, baseArg2)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				protected override void ServiceStop()
				{
					this._enclosing.numStops++;
					this._enclosing.JobHistoryEventHandlerStopped = this._enclosing.numStops;
					base.ServiceStop();
				}

				private readonly TestMRApp _enclosing;
			}

			private readonly TestMRAppComponentDependencies _enclosing;
		}
	}
}
