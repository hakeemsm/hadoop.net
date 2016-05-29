using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.Impl.PB
{
	public class ApplicationStartDataPBImpl : ApplicationStartData
	{
		internal ApplicationHistoryServerProtos.ApplicationStartDataProto proto = ApplicationHistoryServerProtos.ApplicationStartDataProto
			.GetDefaultInstance();

		internal ApplicationHistoryServerProtos.ApplicationStartDataProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private ApplicationId applicationId;

		public ApplicationStartDataPBImpl()
		{
			builder = ApplicationHistoryServerProtos.ApplicationStartDataProto.NewBuilder();
		}

		public ApplicationStartDataPBImpl(ApplicationHistoryServerProtos.ApplicationStartDataProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override ApplicationId GetApplicationId()
		{
			if (this.applicationId != null)
			{
				return this.applicationId;
			}
			ApplicationHistoryServerProtos.ApplicationStartDataProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasApplicationId())
			{
				return null;
			}
			this.applicationId = ConvertFromProtoFormat(p.GetApplicationId());
			return this.applicationId;
		}

		public override void SetApplicationId(ApplicationId applicationId)
		{
			MaybeInitBuilder();
			if (applicationId == null)
			{
				builder.ClearApplicationId();
			}
			this.applicationId = applicationId;
		}

		public override string GetApplicationName()
		{
			ApplicationHistoryServerProtos.ApplicationStartDataProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasApplicationName())
			{
				return null;
			}
			return p.GetApplicationName();
		}

		public override void SetApplicationName(string applicationName)
		{
			MaybeInitBuilder();
			if (applicationName == null)
			{
				builder.ClearApplicationName();
				return;
			}
			builder.SetApplicationName(applicationName);
		}

		public override string GetApplicationType()
		{
			ApplicationHistoryServerProtos.ApplicationStartDataProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasApplicationType())
			{
				return null;
			}
			return p.GetApplicationType();
		}

		public override void SetApplicationType(string applicationType)
		{
			MaybeInitBuilder();
			if (applicationType == null)
			{
				builder.ClearApplicationType();
				return;
			}
			builder.SetApplicationType(applicationType);
		}

		public override string GetUser()
		{
			ApplicationHistoryServerProtos.ApplicationStartDataProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasUser())
			{
				return null;
			}
			return p.GetUser();
		}

		public override void SetUser(string user)
		{
			MaybeInitBuilder();
			if (user == null)
			{
				builder.ClearUser();
				return;
			}
			builder.SetUser(user);
		}

		public override string GetQueue()
		{
			ApplicationHistoryServerProtos.ApplicationStartDataProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasQueue())
			{
				return null;
			}
			return p.GetQueue();
		}

		public override void SetQueue(string queue)
		{
			MaybeInitBuilder();
			if (queue == null)
			{
				builder.ClearQueue();
				return;
			}
			builder.SetQueue(queue);
		}

		public override long GetSubmitTime()
		{
			ApplicationHistoryServerProtos.ApplicationStartDataProtoOrBuilder p = viaProto ? 
				proto : builder;
			return p.GetSubmitTime();
		}

		public override void SetSubmitTime(long submitTime)
		{
			MaybeInitBuilder();
			builder.SetSubmitTime(submitTime);
		}

		public override long GetStartTime()
		{
			ApplicationHistoryServerProtos.ApplicationStartDataProtoOrBuilder p = viaProto ? 
				proto : builder;
			return p.GetStartTime();
		}

		public override void SetStartTime(long startTime)
		{
			MaybeInitBuilder();
			builder.SetStartTime(startTime);
		}

		public virtual ApplicationHistoryServerProtos.ApplicationStartDataProto GetProto(
			)
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((ApplicationHistoryServerProtos.ApplicationStartDataProto
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

		public override string ToString()
		{
			return TextFormat.ShortDebugString(GetProto());
		}

		private void MergeLocalToBuilder()
		{
			if (this.applicationId != null && !((ApplicationIdPBImpl)this.applicationId).GetProto
				().Equals(builder.GetApplicationId()))
			{
				builder.SetApplicationId(ConvertToProtoFormat(this.applicationId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((ApplicationHistoryServerProtos.ApplicationStartDataProto)builder.Build(
				));
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = ApplicationHistoryServerProtos.ApplicationStartDataProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		private YarnProtos.ApplicationIdProto ConvertToProtoFormat(ApplicationId applicationId
			)
		{
			return ((ApplicationIdPBImpl)applicationId).GetProto();
		}

		private ApplicationIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationIdProto 
			applicationId)
		{
			return new ApplicationIdPBImpl(applicationId);
		}
	}
}
