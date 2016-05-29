using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ApplicationResourceUsageReportPBImpl : ApplicationResourceUsageReport
	{
		internal YarnProtos.ApplicationResourceUsageReportProto proto = YarnProtos.ApplicationResourceUsageReportProto
			.GetDefaultInstance();

		internal YarnProtos.ApplicationResourceUsageReportProto.Builder builder = null;

		internal bool viaProto = false;

		internal Resource usedResources;

		internal Resource reservedResources;

		internal Resource neededResources;

		public ApplicationResourceUsageReportPBImpl()
		{
			builder = YarnProtos.ApplicationResourceUsageReportProto.NewBuilder();
		}

		public ApplicationResourceUsageReportPBImpl(YarnProtos.ApplicationResourceUsageReportProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ApplicationResourceUsageReportProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((YarnProtos.ApplicationResourceUsageReportProto)builder
					.Build());
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
			return TextFormat.ShortDebugString(GetProto());
		}

		private void MergeLocalToBuilder()
		{
			if (this.usedResources != null && !((ResourcePBImpl)this.usedResources).GetProto(
				).Equals(builder.GetUsedResources()))
			{
				builder.SetUsedResources(ConvertToProtoFormat(this.usedResources));
			}
			if (this.reservedResources != null && !((ResourcePBImpl)this.reservedResources).GetProto
				().Equals(builder.GetReservedResources()))
			{
				builder.SetReservedResources(ConvertToProtoFormat(this.reservedResources));
			}
			if (this.neededResources != null && !((ResourcePBImpl)this.neededResources).GetProto
				().Equals(builder.GetNeededResources()))
			{
				builder.SetNeededResources(ConvertToProtoFormat(this.neededResources));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ApplicationResourceUsageReportProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = YarnProtos.ApplicationResourceUsageReportProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		public override int GetNumUsedContainers()
		{
			lock (this)
			{
				YarnProtos.ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetNumUsedContainers());
			}
		}

		public override void SetNumUsedContainers(int num_containers)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetNumUsedContainers((num_containers));
			}
		}

		public override int GetNumReservedContainers()
		{
			lock (this)
			{
				YarnProtos.ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetNumReservedContainers());
			}
		}

		public override void SetNumReservedContainers(int num_reserved_containers)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetNumReservedContainers((num_reserved_containers));
			}
		}

		public override Resource GetUsedResources()
		{
			lock (this)
			{
				YarnProtos.ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
				if (this.usedResources != null)
				{
					return this.usedResources;
				}
				if (!p.HasUsedResources())
				{
					return null;
				}
				this.usedResources = ConvertFromProtoFormat(p.GetUsedResources());
				return this.usedResources;
			}
		}

		public override void SetUsedResources(Resource resources)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (resources == null)
				{
					builder.ClearUsedResources();
				}
				this.usedResources = resources;
			}
		}

		public override Resource GetReservedResources()
		{
			lock (this)
			{
				YarnProtos.ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
				if (this.reservedResources != null)
				{
					return this.reservedResources;
				}
				if (!p.HasReservedResources())
				{
					return null;
				}
				this.reservedResources = ConvertFromProtoFormat(p.GetReservedResources());
				return this.reservedResources;
			}
		}

		public override void SetReservedResources(Resource reserved_resources)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (reserved_resources == null)
				{
					builder.ClearReservedResources();
				}
				this.reservedResources = reserved_resources;
			}
		}

		public override Resource GetNeededResources()
		{
			lock (this)
			{
				YarnProtos.ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
				if (this.neededResources != null)
				{
					return this.neededResources;
				}
				if (!p.HasNeededResources())
				{
					return null;
				}
				this.neededResources = ConvertFromProtoFormat(p.GetNeededResources());
				return this.neededResources;
			}
		}

		public override void SetNeededResources(Resource reserved_resources)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (reserved_resources == null)
				{
					builder.ClearNeededResources();
				}
				this.neededResources = reserved_resources;
			}
		}

		public override void SetMemorySeconds(long memory_seconds)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetMemorySeconds(memory_seconds);
			}
		}

		public override long GetMemorySeconds()
		{
			lock (this)
			{
				YarnProtos.ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
				return p.GetMemorySeconds();
			}
		}

		public override void SetVcoreSeconds(long vcore_seconds)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetVcoreSeconds(vcore_seconds);
			}
		}

		public override long GetVcoreSeconds()
		{
			lock (this)
			{
				YarnProtos.ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetVcoreSeconds());
			}
		}

		private ResourcePBImpl ConvertFromProtoFormat(YarnProtos.ResourceProto p)
		{
			return new ResourcePBImpl(p);
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource t)
		{
			return ((ResourcePBImpl)t).GetProto();
		}
	}
}
