using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class RegisterNodeManagerResponsePBImpl : ProtoBase<YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto
		>, RegisterNodeManagerResponse
	{
		internal YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto proto = YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto
			.GetDefaultInstance();

		internal YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private MasterKey containerTokenMasterKey = null;

		private MasterKey nmTokenMasterKey = null;

		private bool rebuild = false;

		public RegisterNodeManagerResponsePBImpl()
		{
			builder = YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto.NewBuilder
				();
		}

		public RegisterNodeManagerResponsePBImpl(YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto GetProto
			()
		{
			if (rebuild)
			{
				MergeLocalToProto();
			}
			proto = viaProto ? proto : ((YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.containerTokenMasterKey != null)
			{
				builder.SetContainerTokenMasterKey(ConvertToProtoFormat(this.containerTokenMasterKey
					));
			}
			if (this.nmTokenMasterKey != null)
			{
				builder.SetNmTokenMasterKey(ConvertToProtoFormat(this.nmTokenMasterKey));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto)builder.
				Build());
			rebuild = false;
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto.NewBuilder
					(proto);
			}
			viaProto = false;
		}

		public virtual MasterKey GetContainerTokenMasterKey()
		{
			YarnServerCommonServiceProtos.RegisterNodeManagerResponseProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (this.containerTokenMasterKey != null)
			{
				return this.containerTokenMasterKey;
			}
			if (!p.HasContainerTokenMasterKey())
			{
				return null;
			}
			this.containerTokenMasterKey = ConvertFromProtoFormat(p.GetContainerTokenMasterKey
				());
			return this.containerTokenMasterKey;
		}

		public virtual void SetContainerTokenMasterKey(MasterKey masterKey)
		{
			MaybeInitBuilder();
			if (masterKey == null)
			{
				builder.ClearContainerTokenMasterKey();
			}
			this.containerTokenMasterKey = masterKey;
			rebuild = true;
		}

		public virtual MasterKey GetNMTokenMasterKey()
		{
			YarnServerCommonServiceProtos.RegisterNodeManagerResponseProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (this.nmTokenMasterKey != null)
			{
				return this.nmTokenMasterKey;
			}
			if (!p.HasNmTokenMasterKey())
			{
				return null;
			}
			this.nmTokenMasterKey = ConvertFromProtoFormat(p.GetNmTokenMasterKey());
			return this.nmTokenMasterKey;
		}

		public virtual void SetNMTokenMasterKey(MasterKey masterKey)
		{
			MaybeInitBuilder();
			if (masterKey == null)
			{
				builder.ClearNmTokenMasterKey();
			}
			this.nmTokenMasterKey = masterKey;
			rebuild = true;
		}

		public virtual string GetDiagnosticsMessage()
		{
			YarnServerCommonServiceProtos.RegisterNodeManagerResponseProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasDiagnosticsMessage())
			{
				return null;
			}
			return p.GetDiagnosticsMessage();
		}

		public virtual void SetDiagnosticsMessage(string diagnosticsMessage)
		{
			MaybeInitBuilder();
			if (diagnosticsMessage == null)
			{
				builder.ClearDiagnosticsMessage();
				return;
			}
			builder.SetDiagnosticsMessage((diagnosticsMessage));
		}

		public virtual string GetRMVersion()
		{
			YarnServerCommonServiceProtos.RegisterNodeManagerResponseProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasRmVersion())
			{
				return null;
			}
			return p.GetRmVersion();
		}

		public virtual void SetRMVersion(string rmVersion)
		{
			MaybeInitBuilder();
			if (rmVersion == null)
			{
				builder.ClearRmIdentifier();
				return;
			}
			builder.SetRmVersion(rmVersion);
		}

		public virtual NodeAction GetNodeAction()
		{
			YarnServerCommonServiceProtos.RegisterNodeManagerResponseProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasNodeAction())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetNodeAction());
		}

		public virtual void SetNodeAction(NodeAction nodeAction)
		{
			MaybeInitBuilder();
			if (nodeAction == null)
			{
				builder.ClearNodeAction();
			}
			else
			{
				builder.SetNodeAction(ConvertToProtoFormat(nodeAction));
			}
			rebuild = true;
		}

		public virtual long GetRMIdentifier()
		{
			YarnServerCommonServiceProtos.RegisterNodeManagerResponseProtoOrBuilder p = viaProto
				 ? proto : builder;
			return (p.GetRmIdentifier());
		}

		public virtual void SetRMIdentifier(long rmIdentifier)
		{
			MaybeInitBuilder();
			builder.SetRmIdentifier(rmIdentifier);
		}

		private NodeAction ConvertFromProtoFormat(YarnServerCommonProtos.NodeActionProto 
			p)
		{
			return NodeAction.ValueOf(p.ToString());
		}

		private YarnServerCommonProtos.NodeActionProto ConvertToProtoFormat(NodeAction t)
		{
			return YarnServerCommonProtos.NodeActionProto.ValueOf(t.ToString());
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
