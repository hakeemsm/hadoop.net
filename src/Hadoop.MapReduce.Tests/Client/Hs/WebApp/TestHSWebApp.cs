using System.Collections.Generic;
using System.IO;
using Com.Google.Inject;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Log;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	public class TestHSWebApp
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestHSWebApp));

		[NUnit.Framework.Test]
		public virtual void TestAppControllerIndex()
		{
			MockAppContext ctx = new MockAppContext(0, 1, 1, 1);
			Injector injector = WebAppTests.CreateMockInjector<AppContext>(ctx);
			HsController controller = injector.GetInstance<HsController>();
			controller.Index();
			NUnit.Framework.Assert.AreEqual(ctx.GetApplicationID().ToString(), controller.Get
				(AMParams.AppId, string.Empty));
		}

		[NUnit.Framework.Test]
		public virtual void TestJobView()
		{
			Log.Info("HsJobPage");
			AppContext appContext = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = TestAMWebApp.GetJobParams(appContext);
			WebAppTests.TestPage<AppContext>(typeof(HsJobPage), appContext, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestTasksView()
		{
			Log.Info("HsTasksPage");
			AppContext appContext = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = TestAMWebApp.GetTaskParams(appContext);
			WebAppTests.TestPage<AppContext>(typeof(HsTasksPage), appContext, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestTaskView()
		{
			Log.Info("HsTaskPage");
			AppContext appContext = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = TestAMWebApp.GetTaskParams(appContext);
			WebAppTests.TestPage<AppContext>(typeof(HsTaskPage), appContext, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestAttemptsWithJobView()
		{
			Log.Info("HsAttemptsPage with data");
			MockAppContext ctx = new MockAppContext(0, 1, 1, 1);
			JobId id = ctx.GetAllJobs().Keys.GetEnumerator().Next();
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[AMParams.JobId] = id.ToString();
			@params[AMParams.TaskType] = "m";
			@params[AMParams.AttemptState] = "SUCCESSFUL";
			WebAppTests.TestPage<AppContext>(typeof(HsAttemptsPage), ctx, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestAttemptsView()
		{
			Log.Info("HsAttemptsPage");
			AppContext appContext = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = TestAMWebApp.GetTaskParams(appContext);
			WebAppTests.TestPage<AppContext>(typeof(HsAttemptsPage), appContext, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestConfView()
		{
			Log.Info("HsConfPage");
			WebAppTests.TestPage<AppContext>(typeof(HsConfPage), new MockAppContext(0, 1, 1, 
				1));
		}

		[NUnit.Framework.Test]
		public virtual void TestAboutView()
		{
			Log.Info("HsAboutPage");
			WebAppTests.TestPage<AppContext>(typeof(HsAboutPage), new MockAppContext(0, 1, 1, 
				1));
		}

		[NUnit.Framework.Test]
		public virtual void TestJobCounterView()
		{
			Log.Info("JobCounterView");
			AppContext appContext = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = TestAMWebApp.GetJobParams(appContext);
			WebAppTests.TestPage<AppContext>(typeof(HsCountersPage), appContext, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestJobCounterViewForKilledJob()
		{
			Log.Info("JobCounterViewForKilledJob");
			AppContext appContext = new MockAppContext(0, 1, 1, 1, true);
			IDictionary<string, string> @params = TestAMWebApp.GetJobParams(appContext);
			WebAppTests.TestPage<AppContext>(typeof(HsCountersPage), appContext, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestSingleCounterView()
		{
			Log.Info("HsSingleCounterPage");
			WebAppTests.TestPage<AppContext>(typeof(HsSingleCounterPage), new MockAppContext(
				0, 1, 1, 1));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLogsView1()
		{
			Log.Info("HsLogsPage");
			Injector injector = WebAppTests.TestPage<AppContext>(typeof(AggregatedLogsPage), 
				new MockAppContext(0, 1, 1, 1));
			PrintWriter spyPw = WebAppTests.GetPrintWriter(injector);
			Org.Mockito.Mockito.Verify(spyPw).Write("Cannot get container logs without a ContainerId"
				);
			Org.Mockito.Mockito.Verify(spyPw).Write("Cannot get container logs without a NodeId"
				);
			Org.Mockito.Mockito.Verify(spyPw).Write("Cannot get container logs without an app owner"
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLogsView2()
		{
			Log.Info("HsLogsPage with data");
			MockAppContext ctx = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[YarnWebParams.ContainerId] = MRApp.NewContainerId(1, 1, 333, 1).ToString(
				);
			@params[YarnWebParams.NmNodename] = NodeId.NewInstance(MockJobs.NmHost, MockJobs.
				NmPort).ToString();
			@params[YarnWebParams.EntityString] = "container_10_0001_01_000001";
			@params[YarnWebParams.AppOwner] = "owner";
			Injector injector = WebAppTests.TestPage<AppContext>(typeof(AggregatedLogsPage), 
				ctx, @params);
			PrintWriter spyPw = WebAppTests.GetPrintWriter(injector);
			Org.Mockito.Mockito.Verify(spyPw).Write("Aggregation is not enabled. Try the nodemanager at "
				 + MockJobs.NmHost + ":" + MockJobs.NmPort);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLogsViewSingle()
		{
			Log.Info("HsLogsPage with params for single log and data limits");
			MockAppContext ctx = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = new Dictionary<string, string>();
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.LogAggregationEnabled, true);
			@params["start"] = "-2048";
			@params["end"] = "-1024";
			@params[YarnWebParams.ContainerLogType] = "syslog";
			@params[YarnWebParams.ContainerId] = MRApp.NewContainerId(1, 1, 333, 1).ToString(
				);
			@params[YarnWebParams.NmNodename] = NodeId.NewInstance(MockJobs.NmHost, MockJobs.
				NmPort).ToString();
			@params[YarnWebParams.EntityString] = "container_10_0001_01_000001";
			@params[YarnWebParams.AppOwner] = "owner";
			Injector injector = WebAppTests.TestPage<AppContext>(typeof(AggregatedLogsPage), 
				ctx, @params, new _AbstractModule_201(conf));
			PrintWriter spyPw = WebAppTests.GetPrintWriter(injector);
			Org.Mockito.Mockito.Verify(spyPw).Write("Logs not available for container_10_0001_01_000001."
				 + " Aggregation may not be complete, " + "Check back later or try the nodemanager at "
				 + MockJobs.NmHost + ":" + MockJobs.NmPort);
		}

		private sealed class _AbstractModule_201 : AbstractModule
		{
			public _AbstractModule_201(Configuration conf)
			{
				this.conf = conf;
			}

			protected override void Configure()
			{
				this.Bind<Configuration>().ToInstance(conf);
			}

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLogsViewBadStartEnd()
		{
			Log.Info("HsLogsPage with bad start/end params");
			MockAppContext ctx = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params["start"] = "foo";
			@params["end"] = "bar";
			@params[YarnWebParams.ContainerId] = MRApp.NewContainerId(1, 1, 333, 1).ToString(
				);
			@params[YarnWebParams.NmNodename] = NodeId.NewInstance(MockJobs.NmHost, MockJobs.
				NmPort).ToString();
			@params[YarnWebParams.EntityString] = "container_10_0001_01_000001";
			@params[YarnWebParams.AppOwner] = "owner";
			Injector injector = WebAppTests.TestPage<AppContext>(typeof(AggregatedLogsPage), 
				ctx, @params);
			PrintWriter spyPw = WebAppTests.GetPrintWriter(injector);
			Org.Mockito.Mockito.Verify(spyPw).Write("Invalid log start value: foo");
			Org.Mockito.Mockito.Verify(spyPw).Write("Invalid log end value: bar");
		}
	}
}
