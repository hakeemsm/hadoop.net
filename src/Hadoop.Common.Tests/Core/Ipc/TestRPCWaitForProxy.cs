using System;
using System.Net;
using System.Threading;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;

namespace Hadoop.Common.Tests.Core.Ipc
{
	/// <summary>tests that the proxy can be interrupted</summary>
	public class TestRPCWaitForProxy : Assert
	{
		private const string Address = "0.0.0.0";

		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestRPCWaitForProxy
			));

		private static readonly Configuration conf = new Configuration();

		/// <summary>
		/// This tests that the time-bounded wait for a proxy operation works, and
		/// times out.
		/// </summary>
		/// <exception cref="System.Exception">any exception other than that which was expected
		/// 	</exception>
		public virtual void TestWaitForProxy()
		{
			TestRPCWaitForProxy.RpcThread worker = new TestRPCWaitForProxy.RpcThread(this, 0);
			worker.Start();
			worker.Join();
			Exception caught = worker.GetCaught();
			NUnit.Framework.Assert.IsNotNull("No exception was raised", caught);
			if (!(caught is ConnectException))
			{
				throw caught;
			}
		}

		/// <summary>
		/// This test sets off a blocking thread and then interrupts it, before
		/// checking that the thread was interrupted
		/// </summary>
		/// <exception cref="System.Exception">any exception other than that which was expected
		/// 	</exception>
		public virtual void TestInterruptedWaitForProxy()
		{
			TestRPCWaitForProxy.RpcThread worker = new TestRPCWaitForProxy.RpcThread(this, 100
				);
			worker.Start();
			Thread.Sleep(1000);
			Assert.True("worker hasn't started", worker.waitStarted);
			worker.Interrupt();
			worker.Join();
			Exception caught = worker.GetCaught();
			NUnit.Framework.Assert.IsNotNull("No exception was raised", caught);
			// looking for the root cause here, which can be wrapped
			// as part of the NetUtils work. Having this test look
			// a the type of exception there would be brittle to improvements
			// in exception diagnostics.
			Exception cause = caught.InnerException;
			if (cause == null)
			{
				// no inner cause, use outer exception as root cause.
				cause = caught;
			}
			if (!(cause is ThreadInterruptedException) && !(cause is ClosedByInterruptException
				))
			{
				throw caught;
			}
		}

		/// <summary>
		/// This thread waits for a proxy for the specified timeout, and retains any
		/// throwable that was raised in the process
		/// </summary>
		private class RpcThread : Thread
		{
			private Exception caught;

			private int connectRetries;

			private volatile bool waitStarted = false;

			private RpcThread(TestRPCWaitForProxy _enclosing, int connectRetries)
			{
				this._enclosing = _enclosing;
				this.connectRetries = connectRetries;
			}

			public override void Run()
			{
				try
				{
					Configuration config = new Configuration(TestRPCWaitForProxy.conf);
					config.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey, this.connectRetries
						);
					config.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesOnSocketTimeoutsKey
						, this.connectRetries);
					this.waitStarted = true;
					TestRPC.TestProtocol proxy = RPC.WaitForProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol
						.versionID, new IPEndPoint(TestRPCWaitForProxy.Address, 20), config, 15000L);
					proxy.Echo(string.Empty);
				}
				catch (Exception throwable)
				{
					this.caught = throwable;
				}
			}

			public virtual Exception GetCaught()
			{
				return this.caught;
			}

			private readonly TestRPCWaitForProxy _enclosing;
		}
	}
}
