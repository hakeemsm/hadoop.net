using System;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	public class TestRMCommunicator
	{
		internal class MockRMCommunicator : RMCommunicator
		{
			public MockRMCommunicator(TestRMCommunicator _enclosing, ClientService clientService
				, AppContext context)
				: base(clientService, context)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected internal override void Heartbeat()
			{
			}

			private readonly TestRMCommunicator _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMContainerAllocatorExceptionIsHandled()
		{
			ClientService mockClientService = Org.Mockito.Mockito.Mock<ClientService>();
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			TestRMCommunicator.MockRMCommunicator mockRMCommunicator = new TestRMCommunicator.MockRMCommunicator
				(this, mockClientService, mockContext);
			RMCommunicator communicator = Org.Mockito.Mockito.Spy(mockRMCommunicator);
			Clock mockClock = Org.Mockito.Mockito.Mock<Clock>();
			Org.Mockito.Mockito.When(mockContext.GetClock()).ThenReturn(mockClock);
			Org.Mockito.Mockito.DoThrow(new RMContainerAllocationException("Test")).DoNothing
				().When(communicator).Heartbeat();
			Org.Mockito.Mockito.When(mockClock.GetTime()).ThenReturn(1L).ThenThrow(new Exception
				("GetClock called second time, when it should not have since the " + "thread should have quit"
				));
			RMCommunicator.AllocatorRunnable testRunnable = new RMCommunicator.AllocatorRunnable
				(this);
			testRunnable.Run();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMContainerAllocatorYarnRuntimeExceptionIsHandled()
		{
			ClientService mockClientService = Org.Mockito.Mockito.Mock<ClientService>();
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			TestRMCommunicator.MockRMCommunicator mockRMCommunicator = new TestRMCommunicator.MockRMCommunicator
				(this, mockClientService, mockContext);
			RMCommunicator communicator = Org.Mockito.Mockito.Spy(mockRMCommunicator);
			Clock mockClock = Org.Mockito.Mockito.Mock<Clock>();
			Org.Mockito.Mockito.When(mockContext.GetClock()).ThenReturn(mockClock);
			Org.Mockito.Mockito.DoThrow(new YarnRuntimeException("Test")).DoNothing().When(communicator
				).Heartbeat();
			Org.Mockito.Mockito.When(mockClock.GetTime()).ThenReturn(1L).ThenAnswer(new _Answer_84
				(communicator)).ThenThrow(new Exception("GetClock called second time, when it should not have since the thread "
				 + "should have quit"));
			RMCommunicator.AllocatorRunnable testRunnable = new RMCommunicator.AllocatorRunnable
				(this);
			testRunnable.Run();
			Org.Mockito.Mockito.Verify(mockClock, Org.Mockito.Mockito.Times(2)).GetTime();
		}

		private sealed class _Answer_84 : Answer<int>
		{
			public _Answer_84(RMCommunicator communicator)
			{
				this.communicator = communicator;
			}

			/// <exception cref="System.Exception"/>
			public int Answer(InvocationOnMock invocation)
			{
				communicator.Stop();
				return 2;
			}

			private readonly RMCommunicator communicator;
		}
	}
}
