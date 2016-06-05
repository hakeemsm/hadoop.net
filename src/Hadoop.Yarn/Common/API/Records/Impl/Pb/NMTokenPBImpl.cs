using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class NMTokenPBImpl : NMToken
	{
		internal YarnServiceProtos.NMTokenProto proto = YarnServiceProtos.NMTokenProto.GetDefaultInstance
			();

		internal YarnServiceProtos.NMTokenProto.Builder builder = null;

		internal bool viaProto = false;

		private Token token = null;

		private NodeId nodeId = null;

		public NMTokenPBImpl()
		{
			builder = YarnServiceProtos.NMTokenProto.NewBuilder();
		}

		public NMTokenPBImpl(YarnServiceProtos.NMTokenProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override NodeId GetNodeId()
		{
			lock (this)
			{
				YarnServiceProtos.NMTokenProtoOrBuilder p = viaProto ? proto : builder;
				if (this.nodeId != null)
				{
					return nodeId;
				}
				if (!p.HasNodeId())
				{
					return null;
				}
				this.nodeId = ConvertFromProtoFormat(p.GetNodeId());
				return nodeId;
			}
		}

		public override void SetNodeId(NodeId nodeId)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (nodeId == null)
				{
					builder.ClearNodeId();
				}
				this.nodeId = nodeId;
			}
		}

		public override Token GetToken()
		{
			lock (this)
			{
				YarnServiceProtos.NMTokenProtoOrBuilder p = viaProto ? proto : builder;
				if (this.token != null)
				{
					return this.token;
				}
				if (!p.HasToken())
				{
					return null;
				}
				this.token = ConvertFromProtoFormat(p.GetToken());
				return token;
			}
		}

		public override void SetToken(Token token)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (token == null)
				{
					builder.ClearToken();
				}
				this.token = token;
			}
		}

		public virtual YarnServiceProtos.NMTokenProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((YarnServiceProtos.NMTokenProto)builder.Build());
				viaProto = true;
				return proto;
			}
		}

		private void MergeLocalToProto()
		{
			lock (this)
			{
				if (viaProto)
				{
					MaybeInitBuilder();
				}
				MergeLocalToBuilder();
				proto = ((YarnServiceProtos.NMTokenProto)builder.Build());
				viaProto = true;
			}
		}

		private void MergeLocalToBuilder()
		{
			lock (this)
			{
				if (this.nodeId != null)
				{
					builder.SetNodeId(ConvertToProtoFormat(nodeId));
				}
				if (this.token != null)
				{
					builder.SetToken(ConvertToProtoFormat(token));
				}
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = YarnServiceProtos.NMTokenProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		private NodeId ConvertFromProtoFormat(YarnProtos.NodeIdProto p)
		{
			lock (this)
			{
				return new NodeIdPBImpl(p);
			}
		}

		private YarnProtos.NodeIdProto ConvertToProtoFormat(NodeId nodeId)
		{
			lock (this)
			{
				return ((NodeIdPBImpl)nodeId).GetProto();
			}
		}

		private SecurityProtos.TokenProto ConvertToProtoFormat(Token token)
		{
			lock (this)
			{
				return ((TokenPBImpl)token).GetProto();
			}
		}

		private Token ConvertFromProtoFormat(SecurityProtos.TokenProto proto)
		{
			lock (this)
			{
				return new TokenPBImpl(proto);
			}
		}
	}
}
