using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	public class TestRPC
	{
		private const string ExceptionMsg = "test error";

		private const string ExceptionCause = "exception cause";

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		[NUnit.Framework.Test]
		public virtual void TestUnknownCall()
		{
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.IpcRpcImpl, typeof(HadoopYarnProtoRPC).FullName);
			YarnRPC rpc = YarnRPC.Create(conf);
			string bindAddr = "localhost:0";
			IPEndPoint addr = NetUtils.CreateSocketAddr(bindAddr);
			Server server = rpc.GetServer(typeof(ContainerManagementProtocol), new TestRPC.DummyContainerManager
				(this), addr, conf, null, 1);
			server.Start();
			// Any unrelated protocol would do
			ApplicationClientProtocol proxy = (ApplicationClientProtocol)rpc.GetProxy(typeof(
				ApplicationClientProtocol), NetUtils.GetConnectAddress(server), conf);
			try
			{
				proxy.GetNewApplication(Records.NewRecord<GetNewApplicationRequest>());
				NUnit.Framework.Assert.Fail("Excepted RPC call to fail with unknown method.");
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Matches("Unknown method getNewApplication called on.*"
					 + "org.apache.hadoop.yarn.proto.ApplicationClientProtocol" + "\\$ApplicationClientProtocolService\\$BlockingInterface protocol."
					));
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHadoopProtoRPC()
		{
			Test(typeof(HadoopYarnProtoRPC).FullName);
		}

		/// <exception cref="System.Exception"/>
		private void Test(string rpcClass)
		{
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.IpcRpcImpl, rpcClass);
			YarnRPC rpc = YarnRPC.Create(conf);
			string bindAddr = "localhost:0";
			IPEndPoint addr = NetUtils.CreateSocketAddr(bindAddr);
			Server server = rpc.GetServer(typeof(ContainerManagementProtocol), new TestRPC.DummyContainerManager
				(this), addr, conf, null, 1);
			server.Start();
			RPC.SetProtocolEngine(conf, typeof(ContainerManagementProtocolPB), typeof(ProtobufRpcEngine
				));
			ContainerManagementProtocol proxy = (ContainerManagementProtocol)rpc.GetProxy(typeof(
				ContainerManagementProtocol), NetUtils.GetConnectAddress(server), conf);
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
			Token containerToken = NewContainerToken(nodeId, Sharpen.Runtime.GetBytesForString
				("password"), containerTokenIdentifier);
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, containerToken);
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			proxy.StartContainers(allRequests);
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(containerId);
			GetContainerStatusesRequest gcsRequest = GetContainerStatusesRequest.NewInstance(
				containerIds);
			GetContainerStatusesResponse response = proxy.GetContainerStatuses(gcsRequest);
			IList<ContainerStatus> statuses = response.GetContainerStatuses();
			//test remote exception
			bool exception = false;
			try
			{
				StopContainersRequest stopRequest = recordFactory.NewRecordInstance<StopContainersRequest
					>();
				stopRequest.SetContainerIds(containerIds);
				proxy.StopContainers(stopRequest);
			}
			catch (YarnException e)
			{
				exception = true;
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(ExceptionMsg));
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(ExceptionCause));
				System.Console.Out.WriteLine("Test Exception is " + e.Message);
			}
			catch (Exception ex)
			{
				Sharpen.Runtime.PrintStackTrace(ex);
			}
			NUnit.Framework.Assert.IsTrue(exception);
			server.Stop();
			NUnit.Framework.Assert.IsNotNull(statuses[0]);
			NUnit.Framework.Assert.AreEqual(ContainerState.Running, statuses[0].GetState());
		}

		public class DummyContainerManager : ContainerManagementProtocol
		{
			private IList<ContainerStatus> statuses = new AList<ContainerStatus>();

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public virtual GetContainerStatusesResponse GetContainerStatuses(GetContainerStatusesRequest
				 request)
			{
				GetContainerStatusesResponse response = TestRPC.recordFactory.NewRecordInstance<GetContainerStatusesResponse
					>();
				response.SetContainerStatuses(this.statuses);
				return response;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public virtual StartContainersResponse StartContainers(StartContainersRequest requests
				)
			{
				StartContainersResponse response = TestRPC.recordFactory.NewRecordInstance<StartContainersResponse
					>();
				foreach (StartContainerRequest request in requests.GetStartContainerRequests())
				{
					Token containerToken = request.GetContainerToken();
					ContainerTokenIdentifier tokenId = null;
					try
					{
						tokenId = TestRPC.NewContainerTokenIdentifier(containerToken);
					}
					catch (IOException e)
					{
						throw RPCUtil.GetRemoteException(e);
					}
					ContainerStatus status = TestRPC.recordFactory.NewRecordInstance<ContainerStatus>
						();
					status.SetState(ContainerState.Running);
					status.SetContainerId(tokenId.GetContainerID());
					status.SetExitStatus(0);
					this.statuses.AddItem(status);
				}
				return response;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public virtual StopContainersResponse StopContainers(StopContainersRequest request
				)
			{
				Exception e = new Exception(TestRPC.ExceptionMsg, new Exception(TestRPC.ExceptionCause
					));
				throw new YarnException(e);
			}

			internal DummyContainerManager(TestRPC _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRPC _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public static ContainerTokenIdentifier NewContainerTokenIdentifier(Token containerToken
			)
		{
			Org.Apache.Hadoop.Security.Token.Token<ContainerTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<ContainerTokenIdentifier>(((byte[])containerToken.GetIdentifier().Array()), ((byte
				[])containerToken.GetPassword().Array()), new Text(containerToken.GetKind()), new 
				Text(containerToken.GetService()));
			return token.DecodeIdentifier();
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Token NewContainerToken(NodeId nodeId
			, byte[] password, ContainerTokenIdentifier tokenIdentifier)
		{
			// RPC layer client expects ip:port as service for tokens
			IPEndPoint addr = NetUtils.CreateSocketAddrForHost(nodeId.GetHost(), nodeId.GetPort
				());
			// NOTE: use SecurityUtil.setTokenService if this becomes a "real" token
			Org.Apache.Hadoop.Yarn.Api.Records.Token containerToken = Org.Apache.Hadoop.Yarn.Api.Records.Token
				.NewInstance(tokenIdentifier.GetBytes(), ContainerTokenIdentifier.Kind.ToString(
				), password, SecurityUtil.BuildTokenService(addr).ToString());
			return containerToken;
		}
	}
}
