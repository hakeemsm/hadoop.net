using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetQueueInfoRequestPBImpl : GetQueueInfoRequest
	{
		internal YarnServiceProtos.GetQueueInfoRequestProto proto = YarnServiceProtos.GetQueueInfoRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetQueueInfoRequestProto.Builder builder = null;

		internal bool viaProto = false;

		public GetQueueInfoRequestPBImpl()
		{
			builder = YarnServiceProtos.GetQueueInfoRequestProto.NewBuilder();
		}

		public GetQueueInfoRequestPBImpl(YarnServiceProtos.GetQueueInfoRequestProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override bool GetIncludeApplications()
		{
			YarnServiceProtos.GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasIncludeApplications()) ? p.GetIncludeApplications() : false;
		}

		public override bool GetIncludeChildQueues()
		{
			YarnServiceProtos.GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasIncludeChildQueues()) ? p.GetIncludeChildQueues() : false;
		}

		public override string GetQueueName()
		{
			YarnServiceProtos.GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasQueueName()) ? p.GetQueueName() : null;
		}

		public override bool GetRecursive()
		{
			YarnServiceProtos.GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasRecursive()) ? p.GetRecursive() : false;
		}

		public override void SetIncludeApplications(bool includeApplications)
		{
			MaybeInitBuilder();
			builder.SetIncludeApplications(includeApplications);
		}

		public override void SetIncludeChildQueues(bool includeChildQueues)
		{
			MaybeInitBuilder();
			builder.SetIncludeChildQueues(includeChildQueues);
		}

		public override void SetQueueName(string queueName)
		{
			MaybeInitBuilder();
			if (queueName == null)
			{
				builder.ClearQueueName();
				return;
			}
			builder.SetQueueName((queueName));
		}

		public override void SetRecursive(bool recursive)
		{
			MaybeInitBuilder();
			builder.SetRecursive(recursive);
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetQueueInfoRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual YarnServiceProtos.GetQueueInfoRequestProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.GetQueueInfoRequestProto)builder.Build
				());
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
	}
}
