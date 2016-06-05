using System;
using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	public class TestContainerLaunchRPC
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestContainerLaunchRPC
			));

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		/*
		* Test that the container launcher rpc times out properly. This is used
		* by both RM to launch an AM as well as an AM to launch containers.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHadoopProtoRPCTimeout()
		{
			TestRPCTimeout(typeof(HadoopYarnProtoRPC).FullName);
		}

		/// <exception cref="System.Exception"/>
		private void TestRPCTimeout(string rpcClass)
		{
			Configuration conf = new Configuration();
			// set timeout low for the test
			conf.SetInt("yarn.rpc.nm-command-timeout", 3000);
			conf.Set(YarnConfiguration.IpcRpcImpl, rpcClass);
			YarnRPC rpc = YarnRPC.Create(conf);
			string bindAddr = "localhost:0";
			IPEndPoint addr = NetUtils.CreateSocketAddr(bindAddr);
			Server server = rpc.GetServer(typeof(ContainerManagementProtocol), new TestContainerLaunchRPC.DummyContainerManager
				(this), addr, conf, null, 1);
			server.Start();
			try
			{
				ContainerManagementProtocol proxy = (ContainerManagementProtocol)rpc.GetProxy(typeof(
					ContainerManagementProtocol), server.GetListenerAddress(), conf);
				ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
					>();
				ApplicationId applicationId = ApplicationId.NewInstance(0, 0);
				ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(applicationId
					, 0);
				ContainerId containerId = ContainerId.NewContainerId(applicationAttemptId, 100);
				NodeId nodeId = NodeId.NewInstance("localhost", 1234);
				Resource resource = Resource.NewInstance(1234, 2);
				ContainerTokenIdentifier containerTokenIdentifier = new ContainerTokenIdentifier(
					containerId, "localhost", "user", resource, Runtime.CurrentTimeMillis() + 10000, 
					42, 42, Priority.NewInstance(0), 0);
				Token containerToken = TestRPC.NewContainerToken(nodeId, Sharpen.Runtime.GetBytesForString
					("password"), containerTokenIdentifier);
				StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
					, containerToken);
				IList<StartContainerRequest> list = new AList<StartContainerRequest>();
				list.AddItem(scRequest);
				StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
				try
				{
					proxy.StartContainers(allRequests);
				}
				catch (Exception e)
				{
					Log.Info(StringUtils.StringifyException(e));
					NUnit.Framework.Assert.AreEqual("Error, exception is not: " + typeof(SocketTimeoutException
						).FullName, typeof(SocketTimeoutException).FullName, e.GetType().FullName);
					return;
				}
			}
			finally
			{
				server.Stop();
			}
			NUnit.Framework.Assert.Fail("timeout exception should have occurred!");
		}

		public class DummyContainerManager : ContainerManagementProtocol
		{
			private ContainerStatus status = null;

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual StartContainersResponse StartContainers(StartContainersRequest requests
				)
			{
				try
				{
					// make the thread sleep to look like its not going to respond
					Sharpen.Thread.Sleep(10000);
				}
				catch (Exception e)
				{
					TestContainerLaunchRPC.Log.Error(e);
					throw new YarnException(e);
				}
				throw new YarnException("Shouldn't happen!!");
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual StopContainersResponse StopContainers(StopContainersRequest requests
				)
			{
				Exception e = new Exception("Dummy function", new Exception("Dummy function cause"
					));
				throw new YarnException(e);
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual GetContainerStatusesResponse GetContainerStatuses(GetContainerStatusesRequest
				 request)
			{
				IList<ContainerStatus> list = new AList<ContainerStatus>();
				list.AddItem(this.status);
				GetContainerStatusesResponse response = GetContainerStatusesResponse.NewInstance(
					list, null);
				return null;
			}

			internal DummyContainerManager(TestContainerLaunchRPC _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestContainerLaunchRPC _enclosing;
		}
	}
}
