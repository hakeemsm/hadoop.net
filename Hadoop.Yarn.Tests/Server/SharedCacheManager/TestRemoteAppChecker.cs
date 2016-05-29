using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	public class TestRemoteAppChecker
	{
		private RemoteAppChecker checker;

		[TearDown]
		public virtual void Cleanup()
		{
			if (checker != null)
			{
				checker.Stop();
			}
		}

		/// <summary>
		/// Creates/initializes/starts a RemoteAppChecker with a spied
		/// DummyYarnClientImpl.
		/// </summary>
		/// <returns>the spied DummyYarnClientImpl in the created AppChecker</returns>
		private YarnClient CreateCheckerWithMockedClient()
		{
			YarnClient client = Org.Mockito.Mockito.Spy(new TestRemoteAppChecker.DummyYarnClientImpl
				(this));
			checker = new RemoteAppChecker(client);
			checker.Init(new Configuration());
			checker.Start();
			return client;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonExistentApp()
		{
			YarnClient client = CreateCheckerWithMockedClient();
			ApplicationId id = ApplicationId.NewInstance(1, 1);
			// test for null
			Org.Mockito.Mockito.DoReturn(null).When(client).GetApplicationReport(id);
			NUnit.Framework.Assert.IsFalse(checker.IsApplicationActive(id));
			// test for ApplicationNotFoundException
			Org.Mockito.Mockito.DoThrow(new ApplicationNotFoundException("Throw!")).When(client
				).GetApplicationReport(id);
			NUnit.Framework.Assert.IsFalse(checker.IsApplicationActive(id));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRunningApp()
		{
			YarnClient client = CreateCheckerWithMockedClient();
			ApplicationId id = ApplicationId.NewInstance(1, 1);
			// create a report and set the state to an active one
			ApplicationReport report = new ApplicationReportPBImpl();
			report.SetYarnApplicationState(YarnApplicationState.Accepted);
			Org.Mockito.Mockito.DoReturn(report).When(client).GetApplicationReport(id);
			NUnit.Framework.Assert.IsTrue(checker.IsApplicationActive(id));
		}

		internal class DummyYarnClientImpl : YarnClientImpl
		{
			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
			}

			// do nothing
			protected override void ServiceStart()
			{
			}

			// do nothing
			protected override void ServiceStop()
			{
			}

			internal DummyYarnClientImpl(TestRemoteAppChecker _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRemoteAppChecker _enclosing;
			// do nothing
		}
	}
}
