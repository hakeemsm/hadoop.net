using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Client.Api.Async;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl
{
	public class TestAMRMClientAsync
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl.TestAMRMClientAsync
			));

		/// <exception cref="System.Exception"/>
		public virtual void TestAMRMClientAsync()
		{
			Configuration conf = new Configuration();
			AtomicBoolean heartbeatBlock = new AtomicBoolean(true);
			IList<ContainerStatus> completed1 = Arrays.AsList(ContainerStatus.NewInstance(NewContainerId
				(0, 0, 0, 0), ContainerState.Complete, string.Empty, 0));
			IList<Container> allocated1 = Arrays.AsList(Container.NewInstance(null, null, null
				, null, null, null));
			AllocateResponse response1 = CreateAllocateResponse(new AList<ContainerStatus>(), 
				allocated1, null);
			AllocateResponse response2 = CreateAllocateResponse(completed1, new AList<Container
				>(), null);
			AllocateResponse emptyResponse = CreateAllocateResponse(new AList<ContainerStatus
				>(), new AList<Container>(), null);
			TestAMRMClientAsync.TestCallbackHandler callbackHandler = new TestAMRMClientAsync.TestCallbackHandler
				(this);
			AMRMClient<AMRMClient.ContainerRequest> client = Org.Mockito.Mockito.Mock<AMRMClientImpl
				>();
			AtomicInteger secondHeartbeatSync = new AtomicInteger(0);
			Org.Mockito.Mockito.When(client.Allocate(Matchers.AnyFloat())).ThenReturn(response1
				).ThenAnswer(new _Answer_89(secondHeartbeatSync, heartbeatBlock, response2)).ThenReturn
				(emptyResponse);
			Org.Mockito.Mockito.When(client.RegisterApplicationMaster(Matchers.AnyString(), Matchers.AnyInt
				(), Matchers.AnyString())).ThenReturn(null);
			Org.Mockito.Mockito.When(client.GetAvailableResources()).ThenAnswer(new _Answer_105
				(client));
			// take client lock to simulate behavior of real impl
			AMRMClientAsync<AMRMClient.ContainerRequest> asyncClient = AMRMClientAsync.CreateAMRMClientAsync
				(client, 20, callbackHandler);
			asyncClient.Init(conf);
			asyncClient.Start();
			asyncClient.RegisterApplicationMaster("localhost", 1234, null);
			// while the CallbackHandler will still only be processing the first response,
			// heartbeater thread should still be sending heartbeats.
			// To test this, wait for the second heartbeat to be received. 
			while (secondHeartbeatSync.Get() < 1)
			{
				Sharpen.Thread.Sleep(10);
			}
			// heartbeat will be blocked. make sure we can call client methods at this
			// time. Checks that heartbeat is not holding onto client lock
			System.Diagnostics.Debug.Assert((secondHeartbeatSync.Get() < 2));
			asyncClient.GetAvailableResources();
			// method returned. now unblock heartbeat
			System.Diagnostics.Debug.Assert((secondHeartbeatSync.Get() < 2));
			lock (heartbeatBlock)
			{
				heartbeatBlock.Set(false);
				Sharpen.Runtime.NotifyAll(heartbeatBlock);
			}
			// allocated containers should come before completed containers
			NUnit.Framework.Assert.AreEqual(null, callbackHandler.TakeCompletedContainers());
			// wait for the allocated containers from the first heartbeat's response
			while (callbackHandler.TakeAllocatedContainers() == null)
			{
				NUnit.Framework.Assert.AreEqual(null, callbackHandler.TakeCompletedContainers());
				Sharpen.Thread.Sleep(10);
			}
			// wait for the completed containers from the second heartbeat's response
			while (callbackHandler.TakeCompletedContainers() == null)
			{
				Sharpen.Thread.Sleep(10);
			}
			asyncClient.Stop();
			NUnit.Framework.Assert.AreEqual(null, callbackHandler.TakeAllocatedContainers());
			NUnit.Framework.Assert.AreEqual(null, callbackHandler.TakeCompletedContainers());
		}

		private sealed class _Answer_89 : Answer<AllocateResponse>
		{
			public _Answer_89(AtomicInteger secondHeartbeatSync, AtomicBoolean heartbeatBlock
				, AllocateResponse response2)
			{
				this.secondHeartbeatSync = secondHeartbeatSync;
				this.heartbeatBlock = heartbeatBlock;
				this.response2 = response2;
			}

			/// <exception cref="System.Exception"/>
			public AllocateResponse Answer(InvocationOnMock invocation)
			{
				secondHeartbeatSync.IncrementAndGet();
				while (heartbeatBlock.Get())
				{
					lock (heartbeatBlock)
					{
						Sharpen.Runtime.Wait(heartbeatBlock);
					}
				}
				secondHeartbeatSync.IncrementAndGet();
				return response2;
			}

			private readonly AtomicInteger secondHeartbeatSync;

			private readonly AtomicBoolean heartbeatBlock;

			private readonly AllocateResponse response2;
		}

		private sealed class _Answer_105 : Answer<Resource>
		{
			public _Answer_105(AMRMClient<AMRMClient.ContainerRequest> client)
			{
				this.client = client;
			}

			/// <exception cref="System.Exception"/>
			public Resource Answer(InvocationOnMock invocation)
			{
				lock (client)
				{
					Sharpen.Thread.Sleep(10);
				}
				return null;
			}

			private readonly AMRMClient<AMRMClient.ContainerRequest> client;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMRMClientAsyncException()
		{
			string exStr = "TestException";
			YarnException mockException = Org.Mockito.Mockito.Mock<YarnException>();
			Org.Mockito.Mockito.When(mockException.Message).ThenReturn(exStr);
			RunHeartBeatThrowOutException(mockException);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMRMClientAsyncRunTimeException()
		{
			string exStr = "TestRunTimeException";
			RuntimeException mockRunTimeException = Org.Mockito.Mockito.Mock<RuntimeException
				>();
			Org.Mockito.Mockito.When(mockRunTimeException.Message).ThenReturn(exStr);
			RunHeartBeatThrowOutException(mockRunTimeException);
		}

		/// <exception cref="System.Exception"/>
		private void RunHeartBeatThrowOutException(Exception ex)
		{
			Configuration conf = new Configuration();
			TestAMRMClientAsync.TestCallbackHandler callbackHandler = new TestAMRMClientAsync.TestCallbackHandler
				(this);
			AMRMClient<AMRMClient.ContainerRequest> client = Org.Mockito.Mockito.Mock<AMRMClientImpl
				>();
			Org.Mockito.Mockito.When(client.Allocate(Matchers.AnyFloat())).ThenThrow(ex);
			AMRMClientAsync<AMRMClient.ContainerRequest> asyncClient = AMRMClientAsync.CreateAMRMClientAsync
				(client, 20, callbackHandler);
			asyncClient.Init(conf);
			asyncClient.Start();
			lock (callbackHandler.notifier)
			{
				asyncClient.RegisterApplicationMaster("localhost", 1234, null);
				while (callbackHandler.savedException == null)
				{
					try
					{
						Sharpen.Runtime.Wait(callbackHandler.notifier);
					}
					catch (Exception e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
				}
			}
			NUnit.Framework.Assert.IsTrue(callbackHandler.savedException.Message.Contains(ex.
				Message));
			asyncClient.Stop();
			// stopping should have joined all threads and completed all callbacks
			NUnit.Framework.Assert.IsTrue(callbackHandler.callbackCount == 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMRMClientAsyncShutDown()
		{
			Configuration conf = new Configuration();
			TestAMRMClientAsync.TestCallbackHandler callbackHandler = new TestAMRMClientAsync.TestCallbackHandler
				(this);
			AMRMClient<AMRMClient.ContainerRequest> client = Org.Mockito.Mockito.Mock<AMRMClientImpl
				>();
			CreateAllocateResponse(new AList<ContainerStatus>(), new AList<Container>(), null
				);
			Org.Mockito.Mockito.When(client.Allocate(Matchers.AnyFloat())).ThenThrow(new ApplicationAttemptNotFoundException
				("app not found, shut down"));
			AMRMClientAsync<AMRMClient.ContainerRequest> asyncClient = AMRMClientAsync.CreateAMRMClientAsync
				(client, 10, callbackHandler);
			asyncClient.Init(conf);
			asyncClient.Start();
			asyncClient.RegisterApplicationMaster("localhost", 1234, null);
			Sharpen.Thread.Sleep(50);
			Org.Mockito.Mockito.Verify(client, Org.Mockito.Mockito.Times(1)).Allocate(Matchers.AnyFloat
				());
			asyncClient.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMRMClientAsyncShutDownWithWaitFor()
		{
			Configuration conf = new Configuration();
			TestAMRMClientAsync.TestCallbackHandler callbackHandler = new TestAMRMClientAsync.TestCallbackHandler
				(this);
			AMRMClient<AMRMClient.ContainerRequest> client = Org.Mockito.Mockito.Mock<AMRMClientImpl
				>();
			Org.Mockito.Mockito.When(client.Allocate(Matchers.AnyFloat())).ThenThrow(new ApplicationAttemptNotFoundException
				("app not found, shut down"));
			AMRMClientAsync<AMRMClient.ContainerRequest> asyncClient = AMRMClientAsync.CreateAMRMClientAsync
				(client, 10, callbackHandler);
			asyncClient.Init(conf);
			asyncClient.Start();
			Supplier<bool> checker = new _Supplier_246(callbackHandler);
			asyncClient.RegisterApplicationMaster("localhost", 1234, null);
			asyncClient.WaitFor(checker);
			asyncClient.Stop();
			// stopping should have joined all threads and completed all callbacks
			NUnit.Framework.Assert.IsTrue(callbackHandler.callbackCount == 0);
			Org.Mockito.Mockito.Verify(client, Org.Mockito.Mockito.Times(1)).Allocate(Matchers.AnyFloat
				());
			asyncClient.Stop();
		}

		private sealed class _Supplier_246 : Supplier<bool>
		{
			public _Supplier_246(TestAMRMClientAsync.TestCallbackHandler callbackHandler)
			{
				this.callbackHandler = callbackHandler;
			}

			public bool Get()
			{
				return callbackHandler.reboot;
			}

			private readonly TestAMRMClientAsync.TestCallbackHandler callbackHandler;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestCallAMRMClientAsyncStopFromCallbackHandler()
		{
			Configuration conf = new Configuration();
			TestAMRMClientAsync.TestCallbackHandler2 callbackHandler = new TestAMRMClientAsync.TestCallbackHandler2
				(this);
			AMRMClient<AMRMClient.ContainerRequest> client = Org.Mockito.Mockito.Mock<AMRMClientImpl
				>();
			IList<ContainerStatus> completed = Arrays.AsList(ContainerStatus.NewInstance(NewContainerId
				(0, 0, 0, 0), ContainerState.Complete, string.Empty, 0));
			AllocateResponse response = CreateAllocateResponse(completed, new AList<Container
				>(), null);
			Org.Mockito.Mockito.When(client.Allocate(Matchers.AnyFloat())).ThenReturn(response
				);
			AMRMClientAsync<AMRMClient.ContainerRequest> asyncClient = AMRMClientAsync.CreateAMRMClientAsync
				(client, 20, callbackHandler);
			callbackHandler.asynClient = asyncClient;
			asyncClient.Init(conf);
			asyncClient.Start();
			lock (callbackHandler.notifier)
			{
				asyncClient.RegisterApplicationMaster("localhost", 1234, null);
				while (callbackHandler.notify == false)
				{
					try
					{
						Sharpen.Runtime.Wait(callbackHandler.notifier);
					}
					catch (Exception e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestCallAMRMClientAsyncStopFromCallbackHandlerWithWaitFor()
		{
			Configuration conf = new Configuration();
			TestAMRMClientAsync.TestCallbackHandler2 callbackHandler = new TestAMRMClientAsync.TestCallbackHandler2
				(this);
			AMRMClient<AMRMClient.ContainerRequest> client = Org.Mockito.Mockito.Mock<AMRMClientImpl
				>();
			IList<ContainerStatus> completed = Arrays.AsList(ContainerStatus.NewInstance(NewContainerId
				(0, 0, 0, 0), ContainerState.Complete, string.Empty, 0));
			AllocateResponse response = CreateAllocateResponse(completed, new AList<Container
				>(), null);
			Org.Mockito.Mockito.When(client.Allocate(Matchers.AnyFloat())).ThenReturn(response
				);
			AMRMClientAsync<AMRMClient.ContainerRequest> asyncClient = AMRMClientAsync.CreateAMRMClientAsync
				(client, 20, callbackHandler);
			callbackHandler.asynClient = asyncClient;
			asyncClient.Init(conf);
			asyncClient.Start();
			Supplier<bool> checker = new _Supplier_320(callbackHandler);
			asyncClient.RegisterApplicationMaster("localhost", 1234, null);
			asyncClient.WaitFor(checker);
			NUnit.Framework.Assert.IsTrue(checker.Get());
		}

		private sealed class _Supplier_320 : Supplier<bool>
		{
			public _Supplier_320(TestAMRMClientAsync.TestCallbackHandler2 callbackHandler)
			{
				this.callbackHandler = callbackHandler;
			}

			public bool Get()
			{
				return callbackHandler.notify;
			}

			private readonly TestAMRMClientAsync.TestCallbackHandler2 callbackHandler;
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void RunCallBackThrowOutException(TestAMRMClientAsync.TestCallbackHandler2
			 callbackHandler)
		{
			Configuration conf = new Configuration();
			AMRMClient<AMRMClient.ContainerRequest> client = Org.Mockito.Mockito.Mock<AMRMClientImpl
				>();
			IList<ContainerStatus> completed = Arrays.AsList(ContainerStatus.NewInstance(NewContainerId
				(0, 0, 0, 0), ContainerState.Complete, string.Empty, 0));
			AllocateResponse response = CreateAllocateResponse(completed, new AList<Container
				>(), null);
			Org.Mockito.Mockito.When(client.Allocate(Matchers.AnyFloat())).ThenReturn(response
				);
			AMRMClientAsync<AMRMClient.ContainerRequest> asyncClient = AMRMClientAsync.CreateAMRMClientAsync
				(client, 20, callbackHandler);
			callbackHandler.asynClient = asyncClient;
			callbackHandler.throwOutException = true;
			asyncClient.Init(conf);
			asyncClient.Start();
			// call register and wait for error callback and stop
			lock (callbackHandler.notifier)
			{
				asyncClient.RegisterApplicationMaster("localhost", 1234, null);
				while (callbackHandler.notify == false)
				{
					try
					{
						Sharpen.Runtime.Wait(callbackHandler.notifier);
					}
					catch (Exception e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
				}
			}
			// verify error invoked
			Org.Mockito.Mockito.Verify(callbackHandler, Org.Mockito.Mockito.Times(0)).GetProgress
				();
			Org.Mockito.Mockito.Verify(callbackHandler, Org.Mockito.Mockito.Times(1)).OnError
				(Matchers.Any<Exception>());
			// sleep to wait for a few heartbeat calls that can trigger callbacks
			Sharpen.Thread.Sleep(50);
			// verify no more invocations after the first one.
			// ie. callback thread has stopped
			Org.Mockito.Mockito.Verify(callbackHandler, Org.Mockito.Mockito.Times(0)).GetProgress
				();
			Org.Mockito.Mockito.Verify(callbackHandler, Org.Mockito.Mockito.Times(1)).OnError
				(Matchers.Any<Exception>());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestCallBackThrowOutException()
		{
			// test exception in callback with app calling stop() on app.onError()
			TestAMRMClientAsync.TestCallbackHandler2 callbackHandler = Org.Mockito.Mockito.Spy
				(new TestAMRMClientAsync.TestCallbackHandler2(this));
			RunCallBackThrowOutException(callbackHandler);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestCallBackThrowOutExceptionNoStop()
		{
			// test exception in callback with app not calling stop() on app.onError()
			TestAMRMClientAsync.TestCallbackHandler2 callbackHandler = Org.Mockito.Mockito.Spy
				(new TestAMRMClientAsync.TestCallbackHandler2(this));
			callbackHandler.stop = false;
			RunCallBackThrowOutException(callbackHandler);
		}

		private AllocateResponse CreateAllocateResponse(IList<ContainerStatus> completed, 
			IList<Container> allocated, IList<NMToken> nmTokens)
		{
			AllocateResponse response = AllocateResponse.NewInstance(0, completed, allocated, 
				new AList<NodeReport>(), null, null, 1, null, nmTokens);
			return response;
		}

		public static ContainerId NewContainerId(int appId, int appAttemptId, long timestamp
			, int containerId)
		{
			ApplicationId applicationId = ApplicationId.NewInstance(timestamp, appId);
			ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, appAttemptId);
			return ContainerId.NewContainerId(applicationAttemptId, containerId);
		}

		private class TestCallbackHandler : AMRMClientAsync.CallbackHandler
		{
			private volatile IList<ContainerStatus> completedContainers;

			private volatile IList<Container> allocatedContainers;

			internal Exception savedException = null;

			internal volatile bool reboot = false;

			internal object notifier = new object();

			internal int callbackCount = 0;

			public virtual IList<ContainerStatus> TakeCompletedContainers()
			{
				IList<ContainerStatus> ret = this.completedContainers;
				if (ret == null)
				{
					return null;
				}
				this.completedContainers = null;
				lock (ret)
				{
					Sharpen.Runtime.Notify(ret);
				}
				return ret;
			}

			public virtual IList<Container> TakeAllocatedContainers()
			{
				IList<Container> ret = this.allocatedContainers;
				if (ret == null)
				{
					return null;
				}
				this.allocatedContainers = null;
				lock (ret)
				{
					Sharpen.Runtime.Notify(ret);
				}
				return ret;
			}

			public virtual void OnContainersCompleted(IList<ContainerStatus> statuses)
			{
				this.completedContainers = statuses;
				// wait for containers to be taken before returning
				lock (this.completedContainers)
				{
					while (this.completedContainers != null)
					{
						try
						{
							Sharpen.Runtime.Wait(this.completedContainers);
						}
						catch (Exception ex)
						{
							TestAMRMClientAsync.Log.Error("Interrupted during wait", ex);
						}
					}
				}
			}

			public virtual void OnContainersAllocated(IList<Container> containers)
			{
				this.allocatedContainers = containers;
				// wait for containers to be taken before returning
				lock (this.allocatedContainers)
				{
					while (this.allocatedContainers != null)
					{
						try
						{
							Sharpen.Runtime.Wait(this.allocatedContainers);
						}
						catch (Exception ex)
						{
							TestAMRMClientAsync.Log.Error("Interrupted during wait", ex);
						}
					}
				}
			}

			public virtual void OnShutdownRequest()
			{
				this.reboot = true;
				lock (this.notifier)
				{
					Sharpen.Runtime.NotifyAll(this.notifier);
				}
			}

			public virtual void OnNodesUpdated(IList<NodeReport> updatedNodes)
			{
			}

			public virtual float GetProgress()
			{
				this.callbackCount++;
				return 0.5f;
			}

			public virtual void OnError(Exception e)
			{
				this.savedException = new Exception(e.Message);
				lock (this.notifier)
				{
					Sharpen.Runtime.NotifyAll(this.notifier);
				}
			}

			internal TestCallbackHandler(TestAMRMClientAsync _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAMRMClientAsync _enclosing;
		}

		private class TestCallbackHandler2 : AMRMClientAsync.CallbackHandler
		{
			internal object notifier = new object();

			internal AMRMClientAsync asynClient;

			internal bool stop = true;

			internal volatile bool notify = false;

			internal bool throwOutException = false;

			public virtual void OnContainersCompleted(IList<ContainerStatus> statuses)
			{
				if (this.throwOutException)
				{
					throw new YarnRuntimeException("Exception from callback handler");
				}
			}

			public virtual void OnContainersAllocated(IList<Container> containers)
			{
			}

			public virtual void OnShutdownRequest()
			{
			}

			public virtual void OnNodesUpdated(IList<NodeReport> updatedNodes)
			{
			}

			public virtual float GetProgress()
			{
				this.CallStopAndNotify();
				return 0;
			}

			public virtual void OnError(Exception e)
			{
				NUnit.Framework.Assert.AreEqual(e.Message, "Exception from callback handler");
				this.CallStopAndNotify();
			}

			internal virtual void CallStopAndNotify()
			{
				if (this.stop)
				{
					this.asynClient.Stop();
				}
				this.notify = true;
				lock (this.notifier)
				{
					Sharpen.Runtime.NotifyAll(this.notifier);
				}
			}

			internal TestCallbackHandler2(TestAMRMClientAsync _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAMRMClientAsync _enclosing;
		}
	}
}
