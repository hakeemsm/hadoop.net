using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// Tests for
	/// <see cref="RetryCache"/>
	/// </summary>
	public class TestRetryCache
	{
		private static readonly byte[] ClientId = ClientId.GetClientId();

		private static int callId = 100;

		private static readonly Random r = new Random();

		private static readonly TestRetryCache.TestServer testServer = new TestRetryCache.TestServer
			();

		[SetUp]
		public virtual void Setup()
		{
			testServer.ResetCounters();
		}

		internal class TestServer
		{
			internal AtomicInteger retryCount = new AtomicInteger();

			internal AtomicInteger operationCount = new AtomicInteger();

			private RetryCache retryCache = new RetryCache("TestRetryCache", 1, 100 * 1000 * 
				1000 * 1000L);

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
			internal virtual int Echo(int input, int failureOutput, long methodTime, bool success
				)
			{
				RetryCache.CacheEntryWithPayload entry = RetryCache.WaitForCompletion(retryCache, 
					null);
				if (entry != null && entry.IsSuccess())
				{
					System.Console.Out.WriteLine("retryCount incremented " + retryCount.Get());
					retryCount.IncrementAndGet();
					return (int)entry.GetPayload();
				}
				try
				{
					operationCount.IncrementAndGet();
					if (methodTime > 0)
					{
						Sharpen.Thread.Sleep(methodTime);
					}
				}
				finally
				{
					RetryCache.SetState(entry, success, input);
				}
				return success ? input : failureOutput;
			}

			internal virtual void ResetCounters()
			{
				retryCount.Set(0);
				operationCount.Set(0);
			}
		}

		public static Server.Call NewCall()
		{
			return new Server.Call(++callId, 1, null, null, RPC.RpcKind.RpcProtocolBuffer, ClientId
				);
		}

		/// <summary>This simlulates a long server retried operations.</summary>
		/// <remarks>
		/// This simlulates a long server retried operations. Multiple threads start an
		/// operation that takes long time and finally succeeds. The retries in this
		/// case end up waiting for the current operation to complete. All the retries
		/// then complete based on the entry in the retry cache.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLongOperationsSuccessful()
		{
			// Test long successful operations
			// There is no entry in cache expected when the first operation starts
			TestOperations(r.Next(), 100, 20, true, false, NewCall());
		}

		/// <summary>This simlulates a long server operation.</summary>
		/// <remarks>
		/// This simlulates a long server operation. Multiple threads start an
		/// operation that takes long time and finally fails. The retries in this case
		/// end up waiting for the current operation to complete. All the retries end
		/// up performing the operation again.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLongOperationsFailure()
		{
			// Test long failed operations
			// There is no entry in cache expected when the first operation starts
			TestOperations(r.Next(), 100, 20, false, false, NewCall());
		}

		/// <summary>This simlulates a short server operation.</summary>
		/// <remarks>
		/// This simlulates a short server operation. Multiple threads start an
		/// operation that takes very short time and finally succeeds. The retries in
		/// this case do not wait long for the current operation to complete. All the
		/// retries then complete based on the entry in the retry cache.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestShortOperationsSuccess()
		{
			// Test long failed operations
			// There is no entry in cache expected when the first operation starts
			TestOperations(r.Next(), 25, 0, false, false, NewCall());
		}

		/// <summary>This simlulates a short server operation.</summary>
		/// <remarks>
		/// This simlulates a short server operation. Multiple threads start an
		/// operation that takes short time and finally fails. The retries in this case
		/// do not wait for the current operation to complete. All the retries end up
		/// performing the operation again.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestShortOperationsFailure()
		{
			// Test long failed operations
			// There is no entry in cache expected when the first operation starts
			TestOperations(r.Next(), 25, 0, false, false, NewCall());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRetryAfterSuccess()
		{
			// Previous operation successfully completed
			Server.Call call = NewCall();
			int input = r.Next();
			Server.GetCurCall().Set(call);
			testServer.Echo(input, input + 1, 5, true);
			TestOperations(input, 25, 0, true, true, call);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRetryAfterFailure()
		{
			// Previous operation failed
			Server.Call call = NewCall();
			int input = r.Next();
			Server.GetCurCall().Set(call);
			testServer.Echo(input, input + 1, 5, false);
			TestOperations(input, 25, 0, false, true, call);
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.ExecutionException"/>
		public virtual void TestOperations(int input, int numberOfThreads, int pause, bool
			 success, bool attemptedBefore, Server.Call call)
		{
			int failureOutput = input + 1;
			ExecutorService executorService = Executors.NewFixedThreadPool(numberOfThreads);
			IList<Future<int>> list = new AList<Future<int>>();
			for (int i = 0; i < numberOfThreads; i++)
			{
				Callable<int> worker = new _Callable_178(call, pause, input, failureOutput, success
					);
				Future<int> submit = executorService.Submit(worker);
				list.AddItem(submit);
			}
			Assert.Equal(numberOfThreads, list.Count);
			foreach (Future<int> future in list)
			{
				if (success)
				{
					Assert.Equal(input, future.Get());
				}
				else
				{
					Assert.Equal(failureOutput, future.Get());
				}
			}
			if (success)
			{
				// If the operation was successful, all the subsequent operations
				// by other threads should be retries. Operation count should be 1.
				int retries = numberOfThreads + (attemptedBefore ? 0 : -1);
				Assert.Equal(1, testServer.operationCount.Get());
				Assert.Equal(retries, testServer.retryCount.Get());
			}
			else
			{
				// If the operation failed, all the subsequent operations
				// should execute once more, hence the retry count should be 0 and
				// operation count should be the number of tries
				int opCount = numberOfThreads + (attemptedBefore ? 1 : 0);
				Assert.Equal(opCount, testServer.operationCount.Get());
				Assert.Equal(0, testServer.retryCount.Get());
			}
		}

		private sealed class _Callable_178 : Callable<int>
		{
			public _Callable_178(Server.Call call, int pause, int input, int failureOutput, bool
				 success)
			{
				this.call = call;
				this.pause = pause;
				this.input = input;
				this.failureOutput = failureOutput;
				this.success = success;
			}

			/// <exception cref="System.Exception"/>
			public int Call()
			{
				Server.GetCurCall().Set(call);
				Assert.Equal(Server.GetCurCall().Get(), call);
				int randomPause = pause == 0 ? pause : TestRetryCache.r.Next(pause);
				return TestRetryCache.testServer.Echo(input, failureOutput, randomPause, success);
			}

			private readonly Server.Call call;

			private readonly int pause;

			private readonly int input;

			private readonly int failureOutput;

			private readonly bool success;
		}
	}
}
