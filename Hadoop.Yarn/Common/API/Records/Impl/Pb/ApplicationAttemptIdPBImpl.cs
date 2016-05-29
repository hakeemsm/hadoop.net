using Com.Google.Common.Base;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ApplicationAttemptIdPBImpl : ApplicationAttemptId
	{
		internal YarnProtos.ApplicationAttemptIdProto proto = null;

		internal YarnProtos.ApplicationAttemptIdProto.Builder builder = null;

		private ApplicationId applicationId = null;

		public ApplicationAttemptIdPBImpl()
		{
			builder = YarnProtos.ApplicationAttemptIdProto.NewBuilder();
		}

		public ApplicationAttemptIdPBImpl(YarnProtos.ApplicationAttemptIdProto proto)
		{
			this.proto = proto;
			this.applicationId = ConvertFromProtoFormat(proto.GetApplicationId());
		}

		public virtual YarnProtos.ApplicationAttemptIdProto GetProto()
		{
			return proto;
		}

		public override int GetAttemptId()
		{
			Preconditions.CheckNotNull(proto);
			return proto.GetAttemptId();
		}

		protected override void SetAttemptId(int attemptId)
		{
			Preconditions.CheckNotNull(builder);
			builder.SetAttemptId(attemptId);
		}

		public override ApplicationId GetApplicationId()
		{
			return this.applicationId;
		}

		protected override void SetApplicationId(ApplicationId appId)
		{
			if (appId != null)
			{
				Preconditions.CheckNotNull(builder);
				builder.SetApplicationId(ConvertToProtoFormat(appId));
			}
			this.applicationId = appId;
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

		protected override void Build()
		{
			proto = ((YarnProtos.ApplicationAttemptIdProto)builder.Build());
			builder = null;
		}
	}
}
