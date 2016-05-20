using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// Tests for
	/// <see cref="RetryCache"/>
	/// </summary>
	public class TestRetryCache
	{
		private static readonly byte[] CLIENT_ID = org.apache.hadoop.ipc.ClientId.getClientId
			();

		private static int callId = 100;

		private static readonly java.util.Random r = new java.util.Random();

		private static readonly org.apache.hadoop.ipc.TestRetryCache.TestServer testServer
			 = new org.apache.hadoop.ipc.TestRetryCache.TestServer();

		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			testServer.resetCounters();
		}

		internal class TestServer
		{
			internal java.util.concurrent.atomic.AtomicInteger retryCount = new java.util.concurrent.atomic.AtomicInteger
				();

			internal java.util.concurrent.atomic.AtomicInteger operationCount = new java.util.concurrent.atomic.AtomicInteger
				();

			private org.apache.hadoop.ipc.RetryCache retryCache = new org.apache.hadoop.ipc.RetryCache
				("TestRetryCache", 1, 100 * 1000 * 1000 * 1000L);

			/// <summary>
			/// A server method implemented using
			/// <see cref="RetryCache"/>
			/// .
			/// </summary>
			/// <param name="input">
			/// is returned back in echo, if
			/// <paramref name="success"/>
			/// is true.
			/// </param>
			/// <param name="failureOuput">
			/// returned on failure, if
			/// <paramref name="success"/>
			/// is false.
			/// </param>
			/// <param name="methodTime">
			/// time taken by the operation. By passing smaller/larger
			/// value one can simulate an operation that takes short/long time.
			/// </param>
			/// <param name="success">whether this operation completes successfully or not</param>
			/// <returns>
			/// return the input parameter
			/// <paramref name="input"/>
			/// , if
			/// <paramref name="success"/>
			/// is
			/// true, else return
			/// <paramref name="failureOutput"/>
			/// .
			/// </returns>
			/// <exception cref="System.Exception"/>
			internal virtual int echo(int input, int failureOutput, long methodTime, bool success
				)
			{
				org.apache.hadoop.ipc.RetryCache.CacheEntryWithPayload entry = org.apache.hadoop.ipc.RetryCache
					.waitForCompletion(retryCache, null);
				if (entry != null && entry.isSuccess())
				{
					System.Console.Out.WriteLine("retryCount incremented " + retryCount.get());
					retryCount.incrementAndGet();
					return (int)entry.getPayload();
				}
				try
				{
					operationCount.incrementAndGet();
					if (methodTime > 0)
					{
						java.lang.Thread.sleep(methodTime);
					}
				}
				finally
				{
					org.apache.hadoop.ipc.RetryCache.setState(entry, success, input);
				}
				return success ? input : failureOutput;
			}

			internal virtual void resetCounters()
			{
				retryCount.set(0);
				operationCount.set(0);
			}
		}

		public static org.apache.hadoop.ipc.Server.Call newCall()
		{
			return new org.apache.hadoop.ipc.Server.Call(++callId, 1, null, null, org.apache.hadoop.ipc.RPC.RpcKind
				.RPC_PROTOCOL_BUFFER, CLIENT_ID);
		}

		/// <summary>This simlulates a long server retried operations.</summary>
		/// <remarks>
		/// This simlulates a long server retried operations. Multiple threads start an
		/// operation that takes long time and finally succeeds. The retries in this
		/// case end up waiting for the current operation to complete. All the retries
		/// then complete based on the entry in the retry cache.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testLongOperationsSuccessful()
		{
			// Test long successful operations
			// There is no entry in cache expected when the first operation starts
			testOperations(r.nextInt(), 100, 20, true, false, newCall());
		}

		/// <summary>This simlulates a long server operation.</summary>
		/// <remarks>
		/// This simlulates a long server operation. Multiple threads start an
		/// operation that takes long time and finally fails. The retries in this case
		/// end up waiting for the current operation to complete. All the retries end
		/// up performing the operation again.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testLongOperationsFailure()
		{
			// Test long failed operations
			// There is no entry in cache expected when the first operation starts
			testOperations(r.nextInt(), 100, 20, false, false, newCall());
		}

		/// <summary>This simlulates a short server operation.</summary>
		/// <remarks>
		/// This simlulates a short server operation. Multiple threads start an
		/// operation that takes very short time and finally succeeds. The retries in
		/// this case do not wait long for the current operation to complete. All the
		/// retries then complete based on the entry in the retry cache.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testShortOperationsSuccess()
		{
			// Test long failed operations
			// There is no entry in cache expected when the first operation starts
			testOperations(r.nextInt(), 25, 0, false, false, newCall());
		}

		/// <summary>This simlulates a short server operation.</summary>
		/// <remarks>
		/// This simlulates a short server operation. Multiple threads start an
		/// operation that takes short time and finally fails. The retries in this case
		/// do not wait for the current operation to complete. All the retries end up
		/// performing the operation again.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testShortOperationsFailure()
		{
			// Test long failed operations
			// There is no entry in cache expected when the first operation starts
			testOperations(r.nextInt(), 25, 0, false, false, newCall());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRetryAfterSuccess()
		{
			// Previous operation successfully completed
			org.apache.hadoop.ipc.Server.Call call = newCall();
			int input = r.nextInt();
			org.apache.hadoop.ipc.Server.getCurCall().set(call);
			testServer.echo(input, input + 1, 5, true);
			testOperations(input, 25, 0, true, true, call);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRetryAfterFailure()
		{
			// Previous operation failed
			org.apache.hadoop.ipc.Server.Call call = newCall();
			int input = r.nextInt();
			org.apache.hadoop.ipc.Server.getCurCall().set(call);
			testServer.echo(input, input + 1, 5, false);
			testOperations(input, 25, 0, false, true, call);
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="java.util.concurrent.ExecutionException"/>
		public virtual void testOperations(int input, int numberOfThreads, int pause, bool
			 success, bool attemptedBefore, org.apache.hadoop.ipc.Server.Call call)
		{
			int failureOutput = input + 1;
			java.util.concurrent.ExecutorService executorService = java.util.concurrent.Executors
				.newFixedThreadPool(numberOfThreads);
			System.Collections.Generic.IList<java.util.concurrent.Future<int>> list = new System.Collections.Generic.List
				<java.util.concurrent.Future<int>>();
			for (int i = 0; i < numberOfThreads; i++)
			{
				java.util.concurrent.Callable<int> worker = new _Callable_178(call, pause, input, 
					failureOutput, success);
				java.util.concurrent.Future<int> submit = executorService.submit(worker);
				list.add(submit);
			}
			NUnit.Framework.Assert.AreEqual(numberOfThreads, list.Count);
			foreach (java.util.concurrent.Future<int> future in list)
			{
				if (success)
				{
					NUnit.Framework.Assert.AreEqual(input, future.get());
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(failureOutput, future.get());
				}
			}
			if (success)
			{
				// If the operation was successful, all the subsequent operations
				// by other threads should be retries. Operation count should be 1.
				int retries = numberOfThreads + (attemptedBefore ? 0 : -1);
				NUnit.Framework.Assert.AreEqual(1, testServer.operationCount.get());
				NUnit.Framework.Assert.AreEqual(retries, testServer.retryCount.get());
			}
			else
			{
				// If the operation failed, all the subsequent operations
				// should execute once more, hence the retry count should be 0 and
				// operation count should be the number of tries
				int opCount = numberOfThreads + (attemptedBefore ? 1 : 0);
				NUnit.Framework.Assert.AreEqual(opCount, testServer.operationCount.get());
				NUnit.Framework.Assert.AreEqual(0, testServer.retryCount.get());
			}
		}

		private sealed class _Callable_178 : java.util.concurrent.Callable<int>
		{
			public _Callable_178(org.apache.hadoop.ipc.Server.Call call, int pause, int input
				, int failureOutput, bool success)
			{
				this.call = call;
				this.pause = pause;
				this.input = input;
				this.failureOutput = failureOutput;
				this.success = success;
			}

			/// <exception cref="System.Exception"/>
			public int call()
			{
				org.apache.hadoop.ipc.Server.getCurCall().set(call);
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ipc.Server.getCurCall().get(), 
					call);
				int randomPause = pause == 0 ? pause : org.apache.hadoop.ipc.TestRetryCache.r.nextInt
					(pause);
				return org.apache.hadoop.ipc.TestRetryCache.testServer.echo(input, failureOutput, 
					randomPause, success);
			}

			private readonly org.apache.hadoop.ipc.Server.Call call;

			private readonly int pause;

			private readonly int input;

			private readonly int failureOutput;

			private readonly bool success;
		}
	}
}
