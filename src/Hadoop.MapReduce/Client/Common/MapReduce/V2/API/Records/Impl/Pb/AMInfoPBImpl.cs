using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB
{
	public class AMInfoPBImpl : ProtoBase<MRProtos.AMInfoProto>, AMInfo
	{
		internal MRProtos.AMInfoProto proto = MRProtos.AMInfoProto.GetDefaultInstance();

		internal MRProtos.AMInfoProto.Builder builder = null;

		internal bool viaProto = false;

		private ApplicationAttemptId appAttemptId;

		private ContainerId containerId;

		public AMInfoPBImpl()
		{
			builder = MRProtos.AMInfoProto.NewBuilder();
		}

		public AMInfoPBImpl(MRProtos.AMInfoProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRProtos.AMInfoProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((MRProtos.AMInfoProto)builder.Build());
				viaProto = true;
				return proto;
			}
		}

		private void MergeLocalToBuilder()
		{
			lock (this)
			{
				if (this.appAttemptId != null && !((ApplicationAttemptIdPBImpl)this.appAttemptId)
					.GetProto().Equals(builder.GetApplicationAttemptId()))
				{
					builder.SetApplicationAttemptId(ConvertToProtoFormat(this.appAttemptId));
				}
				if (this.GetContainerId() != null && !((ContainerIdPBImpl)this.containerId).GetProto
					().Equals(builder.GetContainerId()))
				{
					builder.SetContainerId(ConvertToProtoFormat(this.containerId));
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
				proto = ((MRProtos.AMInfoProto)builder.Build());
				viaProto = true;
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = MRProtos.AMInfoProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		public virtual ApplicationAttemptId GetAppAttemptId()
		{
			lock (this)
			{
				MRProtos.AMInfoProtoOrBuilder p = viaProto ? proto : builder;
				if (appAttemptId != null)
				{
					return appAttemptId;
				}
				// Else via proto
				if (!p.HasApplicationAttemptId())
				{
					return null;
				}
				appAttemptId = ConvertFromProtoFormat(p.GetApplicationAttemptId());
				return appAttemptId;
			}
		}

		public virtual void SetAppAttemptId(ApplicationAttemptId appAttemptId)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (appAttemptId == null)
				{
					builder.ClearApplicationAttemptId();
				}
				this.appAttemptId = appAttemptId;
			}
		}

		public virtual long GetStartTime()
		{
			lock (this)
			{
				MRProtos.AMInfoProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetStartTime());
			}
		}

		public virtual void SetStartTime(long startTime)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetStartTime(startTime);
			}
		}

		public virtual ContainerId GetContainerId()
		{
			lock (this)
			{
				MRProtos.AMInfoProtoOrBuilder p = viaProto ? proto : builder;
				if (containerId != null)
				{
					return containerId;
				}
				// Else via proto
				if (!p.HasContainerId())
				{
					return null;
				}
				containerId = ConvertFromProtoFormat(p.GetContainerId());
				return containerId;
			}
		}

		public virtual void SetContainerId(ContainerId containerId)
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

		public virtual string GetNodeManagerHost()
		{
			lock (this)
			{
				MRProtos.AMInfoProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasNodeManagerHost())
				{
					return null;
				}
				return p.GetNodeManagerHost();
			}
		}

		public virtual void SetNodeManagerHost(string nmHost)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (nmHost == null)
				{
					builder.ClearNodeManagerHost();
					return;
				}
				builder.SetNodeManagerHost(nmHost);
			}
		}

		public virtual int GetNodeManagerPort()
		{
			lock (this)
			{
				MRProtos.AMInfoProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetNodeManagerPort());
			}
		}

		public virtual void SetNodeManagerPort(int nmPort)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetNodeManagerPort(nmPort);
			}
		}

		public virtual int GetNodeManagerHttpPort()
		{
			lock (this)
			{
				MRProtos.AMInfoProtoOrBuilder p = viaProto ? proto : builder;
				return p.GetNodeManagerHttpPort();
			}
		}

		public virtual void SetNodeManagerHttpPort(int httpPort)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetNodeManagerHttpPort(httpPort);
			}
		}

		private ApplicationAttemptIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationAttemptIdProto
			 p)
		{
			return new ApplicationAttemptIdPBImpl(p);
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto p)
		{
			return new ContainerIdPBImpl(p);
		}

		private YarnProtos.ApplicationAttemptIdProto ConvertToProtoFormat(ApplicationAttemptId
			 t)
		{
			return ((ApplicationAttemptIdPBImpl)t).GetProto();
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId t)
		{
			return ((ContainerIdPBImpl)t).GetProto();
		}
	}
}
