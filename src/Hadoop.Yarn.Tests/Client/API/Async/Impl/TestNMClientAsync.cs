using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Client.Api.Async;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl
{
	public class TestNMClientAsync
	{
		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private NMClientAsyncImpl asyncClient;

		private NodeId nodeId;

		private Token containerToken;

		[TearDown]
		public virtual void Teardown()
		{
			ServiceOperations.Stop(asyncClient);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNMClientAsync()
		{
			Configuration conf = new Configuration();
			conf.SetInt(YarnConfiguration.NmClientAsyncThreadPoolMaxSize, 10);
			// Threads to run are more than the max size of the thread pool
			int expectedSuccess = 40;
			int expectedFailure = 40;
			asyncClient = new TestNMClientAsync.MockNMClientAsync1(this, expectedSuccess, expectedFailure
				);
			asyncClient.Init(conf);
			NUnit.Framework.Assert.AreEqual("The max thread pool size is not correctly set", 
				10, asyncClient.maxThreadPoolSize);
			asyncClient.Start();
			for (int i = 0; i < expectedSuccess + expectedFailure; ++i)
			{
				if (i == expectedSuccess)
				{
					while (!((TestNMClientAsync.TestCallbackHandler1)asyncClient.GetCallbackHandler()
						).IsAllSuccessCallsExecuted())
					{
						Sharpen.Thread.Sleep(10);
					}
					asyncClient.SetClient(MockNMClient(1));
				}
				Container container = MockContainer(i);
				ContainerLaunchContext clc = recordFactory.NewRecordInstance<ContainerLaunchContext
					>();
				asyncClient.StartContainerAsync(container, clc);
			}
			while (!((TestNMClientAsync.TestCallbackHandler1)asyncClient.GetCallbackHandler()
				).IsStartAndQueryFailureCallsExecuted())
			{
				Sharpen.Thread.Sleep(10);
			}
			asyncClient.SetClient(MockNMClient(2));
			((TestNMClientAsync.TestCallbackHandler1)asyncClient.GetCallbackHandler()).path =
				 false;
			for (int i_1 = 0; i_1 < expectedFailure; ++i_1)
			{
				Container container = MockContainer(expectedSuccess + expectedFailure + i_1);
				ContainerLaunchContext clc = recordFactory.NewRecordInstance<ContainerLaunchContext
					>();
				asyncClient.StartContainerAsync(container, clc);
			}
			while (!((TestNMClientAsync.TestCallbackHandler1)asyncClient.GetCallbackHandler()
				).IsStopFailureCallsExecuted())
			{
				Sharpen.Thread.Sleep(10);
			}
			foreach (string errorMsg in ((TestNMClientAsync.TestCallbackHandler1)asyncClient.
				GetCallbackHandler()).errorMsgs)
			{
				System.Console.Out.WriteLine(errorMsg);
			}
			NUnit.Framework.Assert.AreEqual("Error occurs in CallbackHandler", 0, ((TestNMClientAsync.TestCallbackHandler1
				)asyncClient.GetCallbackHandler()).errorMsgs.Count);
			foreach (string errorMsg_1 in ((TestNMClientAsync.MockNMClientAsync1)asyncClient)
				.errorMsgs)
			{
				System.Console.Out.WriteLine(errorMsg_1);
			}
			NUnit.Framework.Assert.AreEqual("Error occurs in ContainerEventProcessor", 0, ((TestNMClientAsync.MockNMClientAsync1
				)asyncClient).errorMsgs.Count);
			// When the callback functions are all executed, the event processor threads
			// may still not terminate and the containers may still not removed.
			while (asyncClient.containers.Count > 0)
			{
				Sharpen.Thread.Sleep(10);
			}
			asyncClient.Stop();
			NUnit.Framework.Assert.IsFalse("The thread of Container Management Event Dispatcher is still alive"
				, asyncClient.eventDispatcherThread.IsAlive());
			NUnit.Framework.Assert.IsTrue("The thread pool is not shut down", asyncClient.threadPool
				.IsShutdown());
		}

		private class MockNMClientAsync1 : NMClientAsyncImpl
		{
			private ICollection<string> errorMsgs = Sharpen.Collections.SynchronizedSet(new HashSet
				<string>());

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			protected internal MockNMClientAsync1(TestNMClientAsync _enclosing, int expectedSuccess
				, int expectedFailure)
				: base(typeof(TestNMClientAsync.MockNMClientAsync1).FullName, this._enclosing.MockNMClient
					(0), new TestNMClientAsync.TestCallbackHandler1(this, expectedSuccess, expectedFailure
					))
			{
				this._enclosing = _enclosing;
			}

			private class MockContainerEventProcessor : NMClientAsyncImpl.ContainerEventProcessor
			{
				public MockContainerEventProcessor(MockNMClientAsync1 _enclosing, NMClientAsyncImpl.ContainerEvent
					 @event)
					: base(_enclosing)
				{
					this._enclosing = _enclosing;
				}

				public override void Run()
				{
					try
					{
						base.Run();
					}
					catch (RuntimeException)
					{
						// If the unexpected throwable comes from error callback functions, it
						// will break ContainerEventProcessor.run(). Therefore, monitor
						// the exception here
						this._enclosing.errorMsgs.AddItem("Unexpected throwable from callback functions should"
							 + " be ignored by Container " + this.@event.GetContainerId());
					}
				}

				private readonly MockNMClientAsync1 _enclosing;
			}

			protected internal override NMClientAsyncImpl.ContainerEventProcessor GetContainerEventProcessor
				(NMClientAsyncImpl.ContainerEvent @event)
			{
				return new TestNMClientAsync.MockNMClientAsync1.MockContainerEventProcessor(this, 
					@event);
			}

			private readonly TestNMClientAsync _enclosing;
		}

		private class TestCallbackHandler1 : NMClientAsync.CallbackHandler
		{
			private bool path = true;

			private int expectedSuccess;

			private int expectedFailure;

			private AtomicInteger actualStartSuccess = new AtomicInteger(0);

			private AtomicInteger actualStartFailure = new AtomicInteger(0);

			private AtomicInteger actualQuerySuccess = new AtomicInteger(0);

			private AtomicInteger actualQueryFailure = new AtomicInteger(0);

			private AtomicInteger actualStopSuccess = new AtomicInteger(0);

			private AtomicInteger actualStopFailure = new AtomicInteger(0);

			private AtomicIntegerArray actualStartSuccessArray;

			private AtomicIntegerArray actualStartFailureArray;

			private AtomicIntegerArray actualQuerySuccessArray;

			private AtomicIntegerArray actualQueryFailureArray;

			private AtomicIntegerArray actualStopSuccessArray;

			private AtomicIntegerArray actualStopFailureArray;

			private ICollection<string> errorMsgs = Sharpen.Collections.SynchronizedSet(new HashSet
				<string>());

			public TestCallbackHandler1(TestNMClientAsync _enclosing, int expectedSuccess, int
				 expectedFailure)
			{
				this._enclosing = _enclosing;
				this.expectedSuccess = expectedSuccess;
				this.expectedFailure = expectedFailure;
				this.actualStartSuccessArray = new AtomicIntegerArray(expectedSuccess);
				this.actualStartFailureArray = new AtomicIntegerArray(expectedFailure);
				this.actualQuerySuccessArray = new AtomicIntegerArray(expectedSuccess);
				this.actualQueryFailureArray = new AtomicIntegerArray(expectedFailure);
				this.actualStopSuccessArray = new AtomicIntegerArray(expectedSuccess);
				this.actualStopFailureArray = new AtomicIntegerArray(expectedFailure);
			}

			public virtual void OnContainerStarted(ContainerId containerId, IDictionary<string
				, ByteBuffer> allServiceResponse)
			{
				if (this.path)
				{
					if (containerId.GetId() >= this.expectedSuccess)
					{
						this.errorMsgs.AddItem("Container " + containerId + " should throw the exception onContainerStarted"
							);
						return;
					}
					this.actualStartSuccess.AddAndGet(1);
					this.actualStartSuccessArray.Set(containerId.GetId(), 1);
					// move on to the following success tests
					this._enclosing.asyncClient.GetContainerStatusAsync(containerId, this._enclosing.
						nodeId);
				}
				else
				{
					// move on to the following failure tests
					this._enclosing.asyncClient.StopContainerAsync(containerId, this._enclosing.nodeId
						);
				}
				// Shouldn't crash the test thread
				throw new RuntimeException("Ignorable Exception");
			}

			public virtual void OnContainerStatusReceived(ContainerId containerId, ContainerStatus
				 containerStatus)
			{
				if (containerId.GetId() >= this.expectedSuccess)
				{
					this.errorMsgs.AddItem("Container " + containerId + " should throw the exception onContainerStatusReceived"
						);
					return;
				}
				this.actualQuerySuccess.AddAndGet(1);
				this.actualQuerySuccessArray.Set(containerId.GetId(), 1);
				// move on to the following success tests
				this._enclosing.asyncClient.StopContainerAsync(containerId, this._enclosing.nodeId
					);
				// Shouldn't crash the test thread
				throw new RuntimeException("Ignorable Exception");
			}

			public virtual void OnContainerStopped(ContainerId containerId)
			{
				if (containerId.GetId() >= this.expectedSuccess)
				{
					this.errorMsgs.AddItem("Container " + containerId + " should throw the exception onContainerStopped"
						);
					return;
				}
				this.actualStopSuccess.AddAndGet(1);
				this.actualStopSuccessArray.Set(containerId.GetId(), 1);
				// Shouldn't crash the test thread
				throw new RuntimeException("Ignorable Exception");
			}

			public virtual void OnStartContainerError(ContainerId containerId, Exception t)
			{
				// If the unexpected throwable comes from success callback functions, it
				// will be handled by the error callback functions. Therefore, monitor
				// the exception here
				if (t is RuntimeException)
				{
					this.errorMsgs.AddItem("Unexpected throwable from callback functions should be" +
						 " ignored by Container " + containerId);
				}
				if (containerId.GetId() < this.expectedSuccess)
				{
					this.errorMsgs.AddItem("Container " + containerId + " shouldn't throw the exception onStartContainerError"
						);
					return;
				}
				this.actualStartFailure.AddAndGet(1);
				this.actualStartFailureArray.Set(containerId.GetId() - this.expectedSuccess, 1);
				// move on to the following failure tests
				this._enclosing.asyncClient.GetContainerStatusAsync(containerId, this._enclosing.
					nodeId);
				// Shouldn't crash the test thread
				throw new RuntimeException("Ignorable Exception");
			}

			public virtual void OnStopContainerError(ContainerId containerId, Exception t)
			{
				if (t is RuntimeException)
				{
					this.errorMsgs.AddItem("Unexpected throwable from callback functions should be" +
						 " ignored by Container " + containerId);
				}
				if (containerId.GetId() < this.expectedSuccess + this.expectedFailure)
				{
					this.errorMsgs.AddItem("Container " + containerId + " shouldn't throw the exception onStopContainerError"
						);
					return;
				}
				this.actualStopFailure.AddAndGet(1);
				this.actualStopFailureArray.Set(containerId.GetId() - this.expectedSuccess - this
					.expectedFailure, 1);
				// Shouldn't crash the test thread
				throw new RuntimeException("Ignorable Exception");
			}

			public virtual void OnGetContainerStatusError(ContainerId containerId, Exception 
				t)
			{
				if (t is RuntimeException)
				{
					this.errorMsgs.AddItem("Unexpected throwable from callback functions should be" +
						 " ignored by Container " + containerId);
				}
				if (containerId.GetId() < this.expectedSuccess)
				{
					this.errorMsgs.AddItem("Container " + containerId + " shouldn't throw the exception onGetContainerStatusError"
						);
					return;
				}
				this.actualQueryFailure.AddAndGet(1);
				this.actualQueryFailureArray.Set(containerId.GetId() - this.expectedSuccess, 1);
				// Shouldn't crash the test thread
				throw new RuntimeException("Ignorable Exception");
			}

			public virtual bool IsAllSuccessCallsExecuted()
			{
				bool isAllSuccessCallsExecuted = this.actualStartSuccess.Get() == this.expectedSuccess
					 && this.actualQuerySuccess.Get() == this.expectedSuccess && this.actualStopSuccess
					.Get() == this.expectedSuccess;
				if (isAllSuccessCallsExecuted)
				{
					this.AssertAtomicIntegerArray(this.actualStartSuccessArray);
					this.AssertAtomicIntegerArray(this.actualQuerySuccessArray);
					this.AssertAtomicIntegerArray(this.actualStopSuccessArray);
				}
				return isAllSuccessCallsExecuted;
			}

			public virtual bool IsStartAndQueryFailureCallsExecuted()
			{
				bool isStartAndQueryFailureCallsExecuted = this.actualStartFailure.Get() == this.
					expectedFailure && this.actualQueryFailure.Get() == this.expectedFailure;
				if (isStartAndQueryFailureCallsExecuted)
				{
					this.AssertAtomicIntegerArray(this.actualStartFailureArray);
					this.AssertAtomicIntegerArray(this.actualQueryFailureArray);
				}
				return isStartAndQueryFailureCallsExecuted;
			}

			public virtual bool IsStopFailureCallsExecuted()
			{
				bool isStopFailureCallsExecuted = this.actualStopFailure.Get() == this.expectedFailure;
				if (isStopFailureCallsExecuted)
				{
					this.AssertAtomicIntegerArray(this.actualStopFailureArray);
				}
				return isStopFailureCallsExecuted;
			}

			private void AssertAtomicIntegerArray(AtomicIntegerArray array)
			{
				for (int i = 0; i < array.Length(); ++i)
				{
					NUnit.Framework.Assert.AreEqual(1, array.Get(i));
				}
			}

			private readonly TestNMClientAsync _enclosing;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private NMClient MockNMClient(int mode)
		{
			NMClient client = Org.Mockito.Mockito.Mock<NMClient>();
			switch (mode)
			{
				case 0:
				{
					Org.Mockito.Mockito.When(client.StartContainer(Matchers.Any<Container>(), Matchers.Any
						<ContainerLaunchContext>())).ThenReturn(Sharpen.Collections.EmptyMap<string, ByteBuffer
						>());
					Org.Mockito.Mockito.When(client.GetContainerStatus(Matchers.Any<ContainerId>(), Matchers.Any
						<NodeId>())).ThenReturn(recordFactory.NewRecordInstance<ContainerStatus>());
					Org.Mockito.Mockito.DoNothing().When(client).StopContainer(Matchers.Any<ContainerId
						>(), Matchers.Any<NodeId>());
					break;
				}

				case 1:
				{
					Org.Mockito.Mockito.DoThrow(RPCUtil.GetRemoteException("Start Exception")).When(client
						).StartContainer(Matchers.Any<Container>(), Matchers.Any<ContainerLaunchContext>
						());
					Org.Mockito.Mockito.DoThrow(RPCUtil.GetRemoteException("Query Exception")).When(client
						).GetContainerStatus(Matchers.Any<ContainerId>(), Matchers.Any<NodeId>());
					Org.Mockito.Mockito.DoThrow(RPCUtil.GetRemoteException("Stop Exception")).When(client
						).StopContainer(Matchers.Any<ContainerId>(), Matchers.Any<NodeId>());
					break;
				}

				case 2:
				{
					Org.Mockito.Mockito.When(client.StartContainer(Matchers.Any<Container>(), Matchers.Any
						<ContainerLaunchContext>())).ThenReturn(Sharpen.Collections.EmptyMap<string, ByteBuffer
						>());
					Org.Mockito.Mockito.When(client.GetContainerStatus(Matchers.Any<ContainerId>(), Matchers.Any
						<NodeId>())).ThenReturn(recordFactory.NewRecordInstance<ContainerStatus>());
					Org.Mockito.Mockito.DoThrow(RPCUtil.GetRemoteException("Stop Exception")).When(client
						).StopContainer(Matchers.Any<ContainerId>(), Matchers.Any<NodeId>());
					break;
				}
			}
			return client;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestOutOfOrder()
		{
			CyclicBarrier barrierA = new CyclicBarrier(2);
			CyclicBarrier barrierB = new CyclicBarrier(2);
			CyclicBarrier barrierC = new CyclicBarrier(2);
			asyncClient = new TestNMClientAsync.MockNMClientAsync2(this, barrierA, barrierB, 
				barrierC);
			asyncClient.Init(new Configuration());
			asyncClient.Start();
			Container container = MockContainer(1);
			ContainerLaunchContext clc = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			// start container from another thread
			Sharpen.Thread t = new _Thread_434(this, container, clc);
			t.Start();
			barrierA.Await();
			asyncClient.StopContainerAsync(container.GetId(), container.GetNodeId());
			barrierC.Await();
			NUnit.Framework.Assert.IsFalse("Starting and stopping should be out of order", ((
				TestNMClientAsync.TestCallbackHandler2)asyncClient.GetCallbackHandler()).exceptionOccurred
				.Get());
		}

		private sealed class _Thread_434 : Sharpen.Thread
		{
			public _Thread_434(TestNMClientAsync _enclosing, Container container, ContainerLaunchContext
				 clc)
			{
				this._enclosing = _enclosing;
				this.container = container;
				this.clc = clc;
			}

			public override void Run()
			{
				this._enclosing.asyncClient.StartContainerAsync(container, clc);
			}

			private readonly TestNMClientAsync _enclosing;

			private readonly Container container;

			private readonly ContainerLaunchContext clc;
		}

		private class MockNMClientAsync2 : NMClientAsyncImpl
		{
			private CyclicBarrier barrierA;

			private CyclicBarrier barrierB;

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			protected internal MockNMClientAsync2(TestNMClientAsync _enclosing, CyclicBarrier
				 barrierA, CyclicBarrier barrierB, CyclicBarrier barrierC)
				: base(typeof(TestNMClientAsync.MockNMClientAsync2).FullName, this._enclosing.MockNMClient
					(0), new TestNMClientAsync.TestCallbackHandler2(this, barrierC))
			{
				this._enclosing = _enclosing;
				this.barrierA = barrierA;
				this.barrierB = barrierB;
			}

			private class MockContainerEventProcessor : NMClientAsyncImpl.ContainerEventProcessor
			{
				public MockContainerEventProcessor(MockNMClientAsync2 _enclosing, NMClientAsyncImpl.ContainerEvent
					 @event)
					: base(_enclosing)
				{
					this._enclosing = _enclosing;
				}

				public override void Run()
				{
					try
					{
						if (this.@event.GetType() == NMClientAsyncImpl.ContainerEventType.StartContainer)
						{
							this._enclosing.barrierA.Await();
							this._enclosing.barrierB.Await();
						}
						base.Run();
						if (this.@event.GetType() == NMClientAsyncImpl.ContainerEventType.StopContainer)
						{
							this._enclosing.barrierB.Await();
						}
					}
					catch (Exception e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
					catch (BrokenBarrierException e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
				}

				private readonly MockNMClientAsync2 _enclosing;
			}

			protected internal override NMClientAsyncImpl.ContainerEventProcessor GetContainerEventProcessor
				(NMClientAsyncImpl.ContainerEvent @event)
			{
				return new TestNMClientAsync.MockNMClientAsync2.MockContainerEventProcessor(this, 
					@event);
			}

			private readonly TestNMClientAsync _enclosing;
		}

		private class TestCallbackHandler2 : NMClientAsync.CallbackHandler
		{
			private CyclicBarrier barrierC;

			private AtomicBoolean exceptionOccurred = new AtomicBoolean(false);

			public TestCallbackHandler2(TestNMClientAsync _enclosing, CyclicBarrier barrierC)
			{
				this._enclosing = _enclosing;
				this.barrierC = barrierC;
			}

			public virtual void OnContainerStarted(ContainerId containerId, IDictionary<string
				, ByteBuffer> allServiceResponse)
			{
			}

			public virtual void OnContainerStatusReceived(ContainerId containerId, ContainerStatus
				 containerStatus)
			{
			}

			public virtual void OnContainerStopped(ContainerId containerId)
			{
			}

			public virtual void OnStartContainerError(ContainerId containerId, Exception t)
			{
				if (!t.Message.Equals(NMClientAsyncImpl.StatefulContainer.OutOfOrderTransition.StopBeforeStartErrorMsg
					))
				{
					this.exceptionOccurred.Set(true);
					return;
				}
				try
				{
					this.barrierC.Await();
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
				catch (BrokenBarrierException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}

			public virtual void OnGetContainerStatusError(ContainerId containerId, Exception 
				t)
			{
			}

			public virtual void OnStopContainerError(ContainerId containerId, Exception t)
			{
			}

			private readonly TestNMClientAsync _enclosing;
		}

		private Container MockContainer(int i)
		{
			ApplicationId appId = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 1);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(attemptId, i);
			nodeId = NodeId.NewInstance("localhost", 0);
			// Create an empty record
			containerToken = recordFactory.NewRecordInstance<Token>();
			return Container.NewInstance(containerId, nodeId, null, null, null, containerToken
				);
		}
	}
}
