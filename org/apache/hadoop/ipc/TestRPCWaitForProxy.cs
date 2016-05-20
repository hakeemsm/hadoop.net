using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>tests that the proxy can be interrupted</summary>
	public class TestRPCWaitForProxy : NUnit.Framework.Assert
	{
		private const string ADDRESS = "0.0.0.0";

		private static readonly org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(
			Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPCWaitForProxy
			)));

		private static readonly org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		/// <summary>
		/// This tests that the time-bounded wait for a proxy operation works, and
		/// times out.
		/// </summary>
		/// <exception cref="System.Exception">any exception other than that which was expected
		/// 	</exception>
		public virtual void testWaitForProxy()
		{
			org.apache.hadoop.ipc.TestRPCWaitForProxy.RpcThread worker = new org.apache.hadoop.ipc.TestRPCWaitForProxy.RpcThread
				(this, 0);
			worker.start();
			worker.join();
			System.Exception caught = worker.getCaught();
			NUnit.Framework.Assert.IsNotNull("No exception was raised", caught);
			if (!(caught is java.net.ConnectException))
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
		public virtual void testInterruptedWaitForProxy()
		{
			org.apache.hadoop.ipc.TestRPCWaitForProxy.RpcThread worker = new org.apache.hadoop.ipc.TestRPCWaitForProxy.RpcThread
				(this, 100);
			worker.start();
			java.lang.Thread.sleep(1000);
			NUnit.Framework.Assert.IsTrue("worker hasn't started", worker.waitStarted);
			worker.interrupt();
			worker.join();
			System.Exception caught = worker.getCaught();
			NUnit.Framework.Assert.IsNotNull("No exception was raised", caught);
			// looking for the root cause here, which can be wrapped
			// as part of the NetUtils work. Having this test look
			// a the type of exception there would be brittle to improvements
			// in exception diagnostics.
			System.Exception cause = caught.InnerException;
			if (cause == null)
			{
				// no inner cause, use outer exception as root cause.
				cause = caught;
			}
			if (!(cause is java.io.InterruptedIOException) && !(cause is java.nio.channels.ClosedByInterruptException
				))
			{
				throw caught;
			}
		}

		/// <summary>
		/// This thread waits for a proxy for the specified timeout, and retains any
		/// throwable that was raised in the process
		/// </summary>
		private class RpcThread : java.lang.Thread
		{
			private System.Exception caught;

			private int connectRetries;

			private volatile bool waitStarted = false;

			private RpcThread(TestRPCWaitForProxy _enclosing, int connectRetries)
			{
				this._enclosing = _enclosing;
				this.connectRetries = connectRetries;
			}

			public override void run()
			{
				try
				{
					org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration
						(org.apache.hadoop.ipc.TestRPCWaitForProxy.conf);
					config.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY
						, this.connectRetries);
					config.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY
						, this.connectRetries);
					this.waitStarted = true;
					org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = org.apache.hadoop.ipc.RPC.waitForProxy
						<org.apache.hadoop.ipc.TestRPC.TestProtocol>(org.apache.hadoop.ipc.TestRPC.TestProtocol
						.versionID, new java.net.InetSocketAddress(org.apache.hadoop.ipc.TestRPCWaitForProxy
						.ADDRESS, 20), config, 15000L);
					proxy.echo(string.Empty);
				}
				catch (System.Exception throwable)
				{
					this.caught = throwable;
				}
			}

			public virtual System.Exception getCaught()
			{
				return this.caught;
			}

			private readonly TestRPCWaitForProxy _enclosing;
		}
	}
}
