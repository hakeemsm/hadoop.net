using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetQueueInfoResponsePBImpl : GetQueueInfoResponse
	{
		internal QueueInfo queueInfo;

		internal YarnServiceProtos.GetQueueInfoResponseProto proto = YarnServiceProtos.GetQueueInfoResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetQueueInfoResponseProto.Builder builder = null;

		internal bool viaProto = false;

		public GetQueueInfoResponsePBImpl()
		{
			builder = YarnServiceProtos.GetQueueInfoResponseProto.NewBuilder();
		}

		public GetQueueInfoResponsePBImpl(YarnServiceProtos.GetQueueInfoResponseProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetQueueInfoResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetQueueInfoResponseProto)builder.
				Build());
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

		public override string ToString()
		{
			return TextFormat.ShortDebugString(GetProto());
		}

		public override QueueInfo GetQueueInfo()
		{
			if (this.queueInfo != null)
			{
				return this.queueInfo;
			}
			YarnServiceProtos.GetQueueInfoResponseProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasQueueInfo())
			{
				return null;
			}
			this.queueInfo = ConvertFromProtoFormat(p.GetQueueInfo());
			return this.queueInfo;
		}

		public override void SetQueueInfo(QueueInfo queueInfo)
		{
			MaybeInitBuilder();
			if (queueInfo == null)
			{
				builder.ClearQueueInfo();
			}
			this.queueInfo = queueInfo;
		}

		private void MergeLocalToBuilder()
		{
			if (this.queueInfo != null)
			{
				builder.SetQueueInfo(ConvertToProtoFormat(this.queueInfo));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetQueueInfoResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetQueueInfoResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private QueueInfo ConvertFromProtoFormat(YarnProtos.QueueInfoProto queueInfo)
		{
			return new QueueInfoPBImpl(queueInfo);
		}

		private YarnProtos.QueueInfoProto ConvertToProtoFormat(QueueInfo queueInfo)
		{
			return ((QueueInfoPBImpl)queueInfo).GetProto();
		}
	}
}
