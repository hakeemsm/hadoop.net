using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class NodeHeartbeatRequestPBImpl : NodeHeartbeatRequest
	{
		internal YarnServerCommonServiceProtos.NodeHeartbeatRequestProto proto = YarnServerCommonServiceProtos.NodeHeartbeatRequestProto
			.GetDefaultInstance();

		internal YarnServerCommonServiceProtos.NodeHeartbeatRequestProto.Builder builder = 
			null;

		internal bool viaProto = false;

		private NodeStatus nodeStatus = null;

		private MasterKey lastKnownContainerTokenMasterKey = null;

		private MasterKey lastKnownNMTokenMasterKey = null;

		public NodeHeartbeatRequestPBImpl()
		{
			builder = YarnServerCommonServiceProtos.NodeHeartbeatRequestProto.NewBuilder();
		}

		public NodeHeartbeatRequestPBImpl(YarnServerCommonServiceProtos.NodeHeartbeatRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerCommonServiceProtos.NodeHeartbeatRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerCommonServiceProtos.NodeHeartbeatRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override int GetHashCode()
		{
			return GetProto().GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other == null)
			{
				return false;
			}
			if (other.GetType().IsAssignableFrom(this.GetType()))
			{
				return this.GetProto().Equals(this.GetType().Cast(other).GetProto());
			}
			return false;
		}

		private void MergeLocalToBuilder()
		{
			if (this.nodeStatus != null)
			{
				builder.SetNodeStatus(ConvertToProtoFormat(this.nodeStatus));
			}
			if (this.lastKnownContainerTokenMasterKey != null)
			{
				builder.SetLastKnownContainerTokenMasterKey(ConvertToProtoFormat(this.lastKnownContainerTokenMasterKey
					));
			}
			if (this.lastKnownNMTokenMasterKey != null)
			{
				builder.SetLastKnownNmTokenMasterKey(ConvertToProtoFormat(this.lastKnownNMTokenMasterKey
					));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerCommonServiceProtos.NodeHeartbeatRequestProto)builder.Build()
				);
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerCommonServiceProtos.NodeHeartbeatRequestProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		public override NodeStatus GetNodeStatus()
		{
			YarnServerCommonServiceProtos.NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (this.nodeStatus != null)
			{
				return this.nodeStatus;
			}
			if (!p.HasNodeStatus())
			{
				return null;
			}
			this.nodeStatus = ConvertFromProtoFormat(p.GetNodeStatus());
			return this.nodeStatus;
		}

		public override void SetNodeStatus(NodeStatus nodeStatus)
		{
			MaybeInitBuilder();
			if (nodeStatus == null)
			{
				builder.ClearNodeStatus();
			}
			this.nodeStatus = nodeStatus;
		}

		public override MasterKey GetLastKnownContainerTokenMasterKey()
		{
			YarnServerCommonServiceProtos.NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (this.lastKnownContainerTokenMasterKey != null)
			{
				return this.lastKnownContainerTokenMasterKey;
			}
			if (!p.HasLastKnownContainerTokenMasterKey())
			{
				return null;
			}
			this.lastKnownContainerTokenMasterKey = ConvertFromProtoFormat(p.GetLastKnownContainerTokenMasterKey
				());
			return this.lastKnownContainerTokenMasterKey;
		}

		public override void SetLastKnownContainerTokenMasterKey(MasterKey masterKey)
		{
			MaybeInitBuilder();
			if (masterKey == null)
			{
				builder.ClearLastKnownContainerTokenMasterKey();
			}
			this.lastKnownContainerTokenMasterKey = masterKey;
		}

		public override MasterKey GetLastKnownNMTokenMasterKey()
		{
			YarnServerCommonServiceProtos.NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (this.lastKnownNMTokenMasterKey != null)
			{
				return this.lastKnownNMTokenMasterKey;
			}
			if (!p.HasLastKnownNmTokenMasterKey())
			{
				return null;
			}
			this.lastKnownNMTokenMasterKey = ConvertFromProtoFormat(p.GetLastKnownNmTokenMasterKey
				());
			return this.lastKnownNMTokenMasterKey;
		}

		public override void SetLastKnownNMTokenMasterKey(MasterKey masterKey)
		{
			MaybeInitBuilder();
			if (masterKey == null)
			{
				builder.ClearLastKnownNmTokenMasterKey();
			}
			this.lastKnownNMTokenMasterKey = masterKey;
		}

		private NodeStatusPBImpl ConvertFromProtoFormat(YarnServerCommonProtos.NodeStatusProto
			 p)
		{
			return new NodeStatusPBImpl(p);
		}

		private YarnServerCommonProtos.NodeStatusProto ConvertToProtoFormat(NodeStatus t)
		{
			return ((NodeStatusPBImpl)t).GetProto();
		}

		private MasterKeyPBImpl ConvertFromProtoFormat(YarnServerCommonProtos.MasterKeyProto
			 p)
		{
			return new MasterKeyPBImpl(p);
		}

		private YarnServerCommonProtos.MasterKeyProto ConvertToProtoFormat(MasterKey t)
		{
			return ((MasterKeyPBImpl)t).GetProto();
		}
	}
}
