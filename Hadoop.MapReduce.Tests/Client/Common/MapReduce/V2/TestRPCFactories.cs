using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestRPCFactories
	{
		[NUnit.Framework.Test]
		public virtual void Test()
		{
			TestPbServerFactory();
			TestPbClientFactory();
		}

		private void TestPbServerFactory()
		{
			IPEndPoint addr = new IPEndPoint(0);
			Configuration conf = new Configuration();
			MRClientProtocol instance = new TestRPCFactories.MRClientProtocolTestImpl(this);
			Server server = null;
			try
			{
				server = RpcServerFactoryPBImpl.Get().GetServer(typeof(MRClientProtocol), instance
					, addr, conf, null, 1);
				server.Start();
			}
			catch (YarnRuntimeException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Failed to crete server");
			}
			finally
			{
				server.Stop();
			}
		}

		private void TestPbClientFactory()
		{
			IPEndPoint addr = new IPEndPoint(0);
			System.Console.Error.WriteLine(addr.GetHostName() + addr.Port);
			Configuration conf = new Configuration();
			MRClientProtocol instance = new TestRPCFactories.MRClientProtocolTestImpl(this);
			Server server = null;
			try
			{
				server = RpcServerFactoryPBImpl.Get().GetServer(typeof(MRClientProtocol), instance
					, addr, conf, null, 1);
				server.Start();
				System.Console.Error.WriteLine(server.GetListenerAddress());
				System.Console.Error.WriteLine(NetUtils.GetConnectAddress(server));
				MRClientProtocol client = null;
				try
				{
					client = (MRClientProtocol)RpcClientFactoryPBImpl.Get().GetClient(typeof(MRClientProtocol
						), 1, NetUtils.GetConnectAddress(server), conf);
				}
				catch (YarnRuntimeException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
					NUnit.Framework.Assert.Fail("Failed to crete client");
				}
			}
			catch (YarnRuntimeException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Failed to crete server");
			}
			finally
			{
				server.Stop();
			}
		}

		public class MRClientProtocolTestImpl : MRClientProtocol
		{
			public virtual IPEndPoint GetConnectAddress()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetJobReportResponse GetJobReport(GetJobReportRequest request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskReportResponse GetTaskReport(GetTaskReportRequest request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskAttemptReportResponse GetTaskAttemptReport(GetTaskAttemptReportRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetCountersResponse GetCounters(GetCountersRequest request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskAttemptCompletionEventsResponse GetTaskAttemptCompletionEvents
				(GetTaskAttemptCompletionEventsRequest request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskReportsResponse GetTaskReports(GetTaskReportsRequest request
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetDiagnosticsResponse GetDiagnostics(GetDiagnosticsRequest request
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillJobResponse KillJob(KillJobRequest request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillTaskResponse KillTask(KillTaskRequest request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillTaskAttemptResponse KillTaskAttempt(KillTaskAttemptRequest request
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FailTaskAttemptResponse FailTaskAttempt(FailTaskAttemptRequest request
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest
				 request)
			{
				return null;
			}

			internal MRClientProtocolTestImpl(TestRPCFactories _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRPCFactories _enclosing;
		}
	}
}
