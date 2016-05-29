using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class MoveApplicationAcrossQueuesRequestPBImpl : MoveApplicationAcrossQueuesRequest
	{
		internal YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto proto = YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private ApplicationId applicationId;

		private string targetQueue;

		public MoveApplicationAcrossQueuesRequestPBImpl()
		{
			builder = YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto.NewBuilder();
		}

		public MoveApplicationAcrossQueuesRequestPBImpl(YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override ApplicationId GetApplicationId()
		{
			if (this.applicationId != null)
			{
				return this.applicationId;
			}
			YarnServiceProtos.MoveApplicationAcrossQueuesRequestProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasApplicationId())
			{
				return null;
			}
			this.applicationId = ConvertFromProtoFormat(p.GetApplicationId());
			return this.applicationId;
		}

		public override void SetApplicationId(ApplicationId appId)
		{
			MaybeInitBuilder();
			if (applicationId == null)
			{
				builder.ClearApplicationId();
			}
			applicationId = appId;
		}

		public override string GetTargetQueue()
		{
			if (this.targetQueue != null)
			{
				return this.targetQueue;
			}
			YarnServiceProtos.MoveApplicationAcrossQueuesRequestProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasApplicationId())
			{
				return null;
			}
			this.targetQueue = p.GetTargetQueue();
			return this.targetQueue;
		}

		public override void SetTargetQueue(string queue)
		{
			MaybeInitBuilder();
			if (applicationId == null)
			{
				builder.ClearTargetQueue();
			}
			targetQueue = queue;
		}

		private void MergeLocalToBuilder()
		{
			if (applicationId != null)
			{
				builder.SetApplicationId(ConvertToProtoFormat(this.applicationId));
			}
			if (targetQueue != null)
			{
				builder.SetTargetQueue(this.targetQueue);
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto)builder.Build
				());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto.NewBuilder(proto
					);
			}
			viaProto = false;
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

		private ApplicationIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationIdProto 
			p)
		{
			return new ApplicationIdPBImpl(p);
		}

		private YarnProtos.ApplicationIdProto ConvertToProtoFormat(ApplicationId t)
		{
			return ((ApplicationIdPBImpl)t).GetProto();
		}
	}
}
