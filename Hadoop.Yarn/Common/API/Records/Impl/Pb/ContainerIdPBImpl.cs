using System;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ContainerIdPBImpl : ContainerId
	{
		internal YarnProtos.ContainerIdProto proto = null;

		internal YarnProtos.ContainerIdProto.Builder builder = null;

		private ApplicationAttemptId applicationAttemptId = null;

		public ContainerIdPBImpl()
		{
			builder = YarnProtos.ContainerIdProto.NewBuilder();
		}

		public ContainerIdPBImpl(YarnProtos.ContainerIdProto proto)
		{
			this.proto = proto;
			this.applicationAttemptId = ConvertFromProtoFormat(proto.GetAppAttemptId());
		}

		public virtual YarnProtos.ContainerIdProto GetProto()
		{
			return proto;
		}

		[Obsolete]
		public override int GetId()
		{
			Preconditions.CheckNotNull(proto);
			return (int)proto.GetId();
		}

		public override long GetContainerId()
		{
			Preconditions.CheckNotNull(proto);
			return proto.GetId();
		}

		protected override void SetContainerId(long id)
		{
			Preconditions.CheckNotNull(builder);
			builder.SetId((id));
		}

		public override ApplicationAttemptId GetApplicationAttemptId()
		{
			return this.applicationAttemptId;
		}

		protected override void SetApplicationAttemptId(ApplicationAttemptId atId)
		{
			if (atId != null)
			{
				Preconditions.CheckNotNull(builder);
				builder.SetAppAttemptId(ConvertToProtoFormat(atId));
			}
			this.applicationAttemptId = atId;
		}

		private ApplicationAttemptIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationAttemptIdProto
			 p)
		{
			return new ApplicationAttemptIdPBImpl(p);
		}

		private YarnProtos.ApplicationAttemptIdProto ConvertToProtoFormat(ApplicationAttemptId
			 t)
		{
			return ((ApplicationAttemptIdPBImpl)t).GetProto();
		}

		protected override void Build()
		{
			proto = ((YarnProtos.ContainerIdProto)builder.Build());
			builder = null;
		}
	}
}
