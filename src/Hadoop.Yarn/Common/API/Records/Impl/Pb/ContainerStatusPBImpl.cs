using System.Text;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ContainerStatusPBImpl : ContainerStatus
	{
		internal YarnProtos.ContainerStatusProto proto = YarnProtos.ContainerStatusProto.
			GetDefaultInstance();

		internal YarnProtos.ContainerStatusProto.Builder builder = null;

		internal bool viaProto = false;

		private ContainerId containerId = null;

		public ContainerStatusPBImpl()
		{
			builder = YarnProtos.ContainerStatusProto.NewBuilder();
		}

		public ContainerStatusPBImpl(YarnProtos.ContainerStatusProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ContainerStatusProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((YarnProtos.ContainerStatusProto)builder.Build());
				viaProto = true;
				return proto;
			}
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

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("ContainerStatus: [");
			sb.Append("ContainerId: ").Append(GetContainerId()).Append(", ");
			sb.Append("State: ").Append(GetState()).Append(", ");
			sb.Append("Diagnostics: ").Append(GetDiagnostics()).Append(", ");
			sb.Append("ExitStatus: ").Append(GetExitStatus()).Append(", ");
			sb.Append("]");
			return sb.ToString();
		}

		private void MergeLocalToBuilder()
		{
			if (containerId != null)
			{
				builder.SetContainerId(ConvertToProtoFormat(this.containerId));
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
				proto = ((YarnProtos.ContainerStatusProto)builder.Build());
				viaProto = true;
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = YarnProtos.ContainerStatusProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		public override ContainerState GetState()
		{
			lock (this)
			{
				YarnProtos.ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasState())
				{
					return null;
				}
				return ConvertFromProtoFormat(p.GetState());
			}
		}

		public override void SetState(ContainerState state)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (state == null)
				{
					builder.ClearState();
					return;
				}
				builder.SetState(ConvertToProtoFormat(state));
			}
		}

		public override ContainerId GetContainerId()
		{
			lock (this)
			{
				YarnProtos.ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
				if (this.containerId != null)
				{
					return this.containerId;
				}
				if (!p.HasContainerId())
				{
					return null;
				}
				this.containerId = ConvertFromProtoFormat(p.GetContainerId());
				return this.containerId;
			}
		}

		public override void SetContainerId(ContainerId containerId)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (containerId == null)
				{
					builder.ClearContainerId();
				}
				this.containerId = containerId;
			}
		}

		public override int GetExitStatus()
		{
			lock (this)
			{
				YarnProtos.ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
				return p.GetExitStatus();
			}
		}

		public override void SetExitStatus(int exitStatus)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetExitStatus(exitStatus);
			}
		}

		public override string GetDiagnostics()
		{
			lock (this)
			{
				YarnProtos.ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetDiagnostics());
			}
		}

		public override void SetDiagnostics(string diagnostics)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetDiagnostics(diagnostics);
			}
		}

		private YarnProtos.ContainerStateProto ConvertToProtoFormat(ContainerState e)
		{
			return ProtoUtils.ConvertToProtoFormat(e);
		}

		private ContainerState ConvertFromProtoFormat(YarnProtos.ContainerStateProto e)
		{
			return ProtoUtils.ConvertFromProtoFormat(e);
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto p)
		{
			return new ContainerIdPBImpl(p);
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId t)
		{
			return ((ContainerIdPBImpl)t).GetProto();
		}
	}
}
