using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB
{
	public class JobIdPBImpl : JobId
	{
		internal MRProtos.JobIdProto proto = MRProtos.JobIdProto.GetDefaultInstance();

		internal MRProtos.JobIdProto.Builder builder = null;

		internal bool viaProto = false;

		private ApplicationId applicationId = null;

		public JobIdPBImpl()
		{
			builder = MRProtos.JobIdProto.NewBuilder();
		}

		public JobIdPBImpl(MRProtos.JobIdProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual MRProtos.JobIdProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((MRProtos.JobIdProto)builder.Build());
				viaProto = true;
				return proto;
			}
		}

		private void MergeLocalToBuilder()
		{
			lock (this)
			{
				if (this.applicationId != null && !((ApplicationIdPBImpl)this.applicationId).GetProto
					().Equals(builder.GetAppId()))
				{
					builder.SetAppId(ConvertToProtoFormat(this.applicationId));
				}
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
				proto = ((MRProtos.JobIdProto)builder.Build());
				viaProto = true;
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = MRProtos.JobIdProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		public override ApplicationId GetAppId()
		{
			lock (this)
			{
				MRProtos.JobIdProtoOrBuilder p = viaProto ? proto : builder;
				if (applicationId != null)
				{
					return applicationId;
				}
				// Else via proto
				if (!p.HasAppId())
				{
					return null;
				}
				applicationId = ConvertFromProtoFormat(p.GetAppId());
				return applicationId;
			}
		}

		public override void SetAppId(ApplicationId appId)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (appId == null)
				{
					builder.ClearAppId();
				}
				this.applicationId = appId;
			}
		}

		public override int GetId()
		{
			lock (this)
			{
				MRProtos.JobIdProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetId());
			}
		}

		public override void SetId(int id)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetId((id));
			}
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
