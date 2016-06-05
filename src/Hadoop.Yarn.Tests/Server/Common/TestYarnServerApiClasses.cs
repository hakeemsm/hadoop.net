using System;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	/// <summary>Simple test classes from org.apache.hadoop.yarn.server.api</summary>
	public class TestYarnServerApiClasses
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		/// <summary>Test RegisterNodeManagerResponsePBImpl.</summary>
		/// <remarks>
		/// Test RegisterNodeManagerResponsePBImpl. Test getters and setters. The
		/// RegisterNodeManagerResponsePBImpl should generate a prototype and data
		/// restore from prototype
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestRegisterNodeManagerResponsePBImpl()
		{
			RegisterNodeManagerResponsePBImpl original = new RegisterNodeManagerResponsePBImpl
				();
			original.SetContainerTokenMasterKey(GetMasterKey());
			original.SetNMTokenMasterKey(GetMasterKey());
			original.SetNodeAction(NodeAction.Normal);
			original.SetDiagnosticsMessage("testDiagnosticMessage");
			RegisterNodeManagerResponsePBImpl copy = new RegisterNodeManagerResponsePBImpl(original
				.GetProto());
			NUnit.Framework.Assert.AreEqual(1, copy.GetContainerTokenMasterKey().GetKeyId());
			NUnit.Framework.Assert.AreEqual(1, copy.GetNMTokenMasterKey().GetKeyId());
			NUnit.Framework.Assert.AreEqual(NodeAction.Normal, copy.GetNodeAction());
			NUnit.Framework.Assert.AreEqual("testDiagnosticMessage", copy.GetDiagnosticsMessage
				());
		}

		/// <summary>Test NodeHeartbeatRequestPBImpl.</summary>
		[NUnit.Framework.Test]
		public virtual void TestNodeHeartbeatRequestPBImpl()
		{
			NodeHeartbeatRequestPBImpl original = new NodeHeartbeatRequestPBImpl();
			original.SetLastKnownContainerTokenMasterKey(GetMasterKey());
			original.SetLastKnownNMTokenMasterKey(GetMasterKey());
			original.SetNodeStatus(GetNodeStatus());
			NodeHeartbeatRequestPBImpl copy = new NodeHeartbeatRequestPBImpl(original.GetProto
				());
			NUnit.Framework.Assert.AreEqual(1, copy.GetLastKnownContainerTokenMasterKey().GetKeyId
				());
			NUnit.Framework.Assert.AreEqual(1, copy.GetLastKnownNMTokenMasterKey().GetKeyId()
				);
			NUnit.Framework.Assert.AreEqual("localhost", copy.GetNodeStatus().GetNodeId().GetHost
				());
		}

		/// <summary>Test NodeHeartbeatResponsePBImpl.</summary>
		[NUnit.Framework.Test]
		public virtual void TestNodeHeartbeatResponsePBImpl()
		{
			NodeHeartbeatResponsePBImpl original = new NodeHeartbeatResponsePBImpl();
			original.SetDiagnosticsMessage("testDiagnosticMessage");
			original.SetContainerTokenMasterKey(GetMasterKey());
			original.SetNMTokenMasterKey(GetMasterKey());
			original.SetNextHeartBeatInterval(1000);
			original.SetNodeAction(NodeAction.Normal);
			original.SetResponseId(100);
			NodeHeartbeatResponsePBImpl copy = new NodeHeartbeatResponsePBImpl(original.GetProto
				());
			NUnit.Framework.Assert.AreEqual(100, copy.GetResponseId());
			NUnit.Framework.Assert.AreEqual(NodeAction.Normal, copy.GetNodeAction());
			NUnit.Framework.Assert.AreEqual(1000, copy.GetNextHeartBeatInterval());
			NUnit.Framework.Assert.AreEqual(1, copy.GetContainerTokenMasterKey().GetKeyId());
			NUnit.Framework.Assert.AreEqual(1, copy.GetNMTokenMasterKey().GetKeyId());
			NUnit.Framework.Assert.AreEqual("testDiagnosticMessage", copy.GetDiagnosticsMessage
				());
		}

		/// <summary>Test RegisterNodeManagerRequestPBImpl.</summary>
		[NUnit.Framework.Test]
		public virtual void TestRegisterNodeManagerRequestPBImpl()
		{
			RegisterNodeManagerRequestPBImpl original = new RegisterNodeManagerRequestPBImpl(
				);
			original.SetHttpPort(8080);
			original.SetNodeId(GetNodeId());
			Resource resource = recordFactory.NewRecordInstance<Resource>();
			resource.SetMemory(10000);
			resource.SetVirtualCores(2);
			original.SetResource(resource);
			RegisterNodeManagerRequestPBImpl copy = new RegisterNodeManagerRequestPBImpl(original
				.GetProto());
			NUnit.Framework.Assert.AreEqual(8080, copy.GetHttpPort());
			NUnit.Framework.Assert.AreEqual(9090, copy.GetNodeId().GetPort());
			NUnit.Framework.Assert.AreEqual(10000, copy.GetResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(2, copy.GetResource().GetVirtualCores());
		}

		/// <summary>Test MasterKeyPBImpl.</summary>
		[NUnit.Framework.Test]
		public virtual void TestMasterKeyPBImpl()
		{
			MasterKeyPBImpl original = new MasterKeyPBImpl();
			original.SetBytes(ByteBuffer.Allocate(0));
			original.SetKeyId(1);
			MasterKeyPBImpl copy = new MasterKeyPBImpl(original.GetProto());
			NUnit.Framework.Assert.AreEqual(1, copy.GetKeyId());
			NUnit.Framework.Assert.IsTrue(original.Equals(copy));
			NUnit.Framework.Assert.AreEqual(original.GetHashCode(), copy.GetHashCode());
		}

		/// <summary>Test SerializedExceptionPBImpl.</summary>
		[NUnit.Framework.Test]
		public virtual void TestSerializedExceptionPBImpl()
		{
			SerializedExceptionPBImpl original = new SerializedExceptionPBImpl();
			original.Init("testMessage");
			SerializedExceptionPBImpl copy = new SerializedExceptionPBImpl(original.GetProto(
				));
			NUnit.Framework.Assert.AreEqual("testMessage", copy.GetMessage());
			original = new SerializedExceptionPBImpl();
			original.Init("testMessage", new Exception(new Exception("parent")));
			copy = new SerializedExceptionPBImpl(original.GetProto());
			NUnit.Framework.Assert.AreEqual("testMessage", copy.GetMessage());
			NUnit.Framework.Assert.AreEqual("parent", copy.GetCause().GetMessage());
			NUnit.Framework.Assert.IsTrue(copy.GetRemoteTrace().StartsWith("java.lang.Throwable: java.lang.Throwable: parent"
				));
		}

		/// <summary>Test NodeStatusPBImpl.</summary>
		[NUnit.Framework.Test]
		public virtual void TestNodeStatusPBImpl()
		{
			NodeStatusPBImpl original = new NodeStatusPBImpl();
			original.SetContainersStatuses(Arrays.AsList(GetContainerStatus(1, 2, 1), GetContainerStatus
				(2, 3, 1)));
			original.SetKeepAliveApplications(Arrays.AsList(GetApplicationId(3), GetApplicationId
				(4)));
			original.SetNodeHealthStatus(GetNodeHealthStatus());
			original.SetNodeId(GetNodeId());
			original.SetResponseId(1);
			NodeStatusPBImpl copy = new NodeStatusPBImpl(original.GetProto());
			NUnit.Framework.Assert.AreEqual(3L, copy.GetContainersStatuses()[1].GetContainerId
				().GetContainerId());
			NUnit.Framework.Assert.AreEqual(3, copy.GetKeepAliveApplications()[0].GetId());
			NUnit.Framework.Assert.AreEqual(1000, copy.GetNodeHealthStatus().GetLastHealthReportTime
				());
			NUnit.Framework.Assert.AreEqual(9090, copy.GetNodeId().GetPort());
			NUnit.Framework.Assert.AreEqual(1, copy.GetResponseId());
		}

		private ContainerStatus GetContainerStatus(int applicationId, int containerID, int
			 appAttemptId)
		{
			ContainerStatus status = recordFactory.NewRecordInstance<ContainerStatus>();
			status.SetContainerId(GetContainerId(containerID, appAttemptId));
			return status;
		}

		private ApplicationAttemptId GetApplicationAttemptId(int appAttemptId)
		{
			ApplicationAttemptId result = ApplicationAttemptIdPBImpl.NewInstance(GetApplicationId
				(appAttemptId), appAttemptId);
			return result;
		}

		private ContainerId GetContainerId(int containerID, int appAttemptId)
		{
			ContainerId containerId = ContainerIdPBImpl.NewContainerId(GetApplicationAttemptId
				(appAttemptId), containerID);
			return containerId;
		}

		private ApplicationId GetApplicationId(int applicationId)
		{
			ApplicationIdPBImpl appId = new _ApplicationIdPBImpl_232().SetParameters(applicationId
				, 1000);
			return new ApplicationIdPBImpl(appId.GetProto());
		}

		private sealed class _ApplicationIdPBImpl_232 : ApplicationIdPBImpl
		{
			public _ApplicationIdPBImpl_232()
			{
			}

			public ApplicationIdPBImpl SetParameters(int id, long timestamp)
			{
				this.SetClusterTimestamp(timestamp);
				this.SetId(id);
				this.Build();
				return this;
			}
		}

		private NodeStatus GetNodeStatus()
		{
			NodeStatus status = recordFactory.NewRecordInstance<NodeStatus>();
			status.SetContainersStatuses(new AList<ContainerStatus>());
			status.SetKeepAliveApplications(new AList<ApplicationId>());
			status.SetNodeHealthStatus(GetNodeHealthStatus());
			status.SetNodeId(GetNodeId());
			status.SetResponseId(1);
			return status;
		}

		private NodeId GetNodeId()
		{
			return NodeId.NewInstance("localhost", 9090);
		}

		private NodeHealthStatus GetNodeHealthStatus()
		{
			NodeHealthStatus healStatus = recordFactory.NewRecordInstance<NodeHealthStatus>();
			healStatus.SetHealthReport("healthReport");
			healStatus.SetIsNodeHealthy(true);
			healStatus.SetLastHealthReportTime(1000);
			return healStatus;
		}

		private MasterKey GetMasterKey()
		{
			MasterKey key = recordFactory.NewRecordInstance<MasterKey>();
			key.SetBytes(ByteBuffer.Allocate(0));
			key.SetKeyId(1);
			return key;
		}
	}
}
