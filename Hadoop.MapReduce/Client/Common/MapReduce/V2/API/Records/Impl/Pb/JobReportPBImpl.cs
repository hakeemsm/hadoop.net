using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB
{
	public class JobReportPBImpl : ProtoBase<MRProtos.JobReportProto>, JobReport
	{
		internal MRProtos.JobReportProto proto = MRProtos.JobReportProto.GetDefaultInstance
			();

		internal MRProtos.JobReportProto.Builder builder = null;

		internal bool viaProto = false;

		private JobId jobId = null;

		private IList<AMInfo> amInfos = null;

		public JobReportPBImpl()
		{
			builder = MRProtos.JobReportProto.NewBuilder();
		}

		public JobReportPBImpl(MRProtos.JobReportProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRProtos.JobReportProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((MRProtos.JobReportProto)builder.Build());
				viaProto = true;
				return proto;
			}
		}

		private void MergeLocalToBuilder()
		{
			lock (this)
			{
				if (this.jobId != null)
				{
					builder.SetJobId(ConvertToProtoFormat(this.jobId));
				}
				if (this.amInfos != null)
				{
					AddAMInfosToProto();
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
				proto = ((MRProtos.JobReportProto)builder.Build());
				viaProto = true;
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = MRProtos.JobReportProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		public virtual JobId GetJobId()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				if (this.jobId != null)
				{
					return this.jobId;
				}
				if (!p.HasJobId())
				{
					return null;
				}
				this.jobId = ConvertFromProtoFormat(p.GetJobId());
				return this.jobId;
			}
		}

		public virtual void SetJobId(JobId jobId)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (jobId == null)
				{
					builder.ClearJobId();
				}
				this.jobId = jobId;
			}
		}

		public virtual JobState GetJobState()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasJobState())
				{
					return null;
				}
				return ConvertFromProtoFormat(p.GetJobState());
			}
		}

		public virtual void SetJobState(JobState jobState)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (jobState == null)
				{
					builder.ClearJobState();
					return;
				}
				builder.SetJobState(ConvertToProtoFormat(jobState));
			}
		}

		public virtual float GetMapProgress()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetMapProgress());
			}
		}

		public virtual void SetMapProgress(float mapProgress)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetMapProgress((mapProgress));
			}
		}

		public virtual float GetReduceProgress()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetReduceProgress());
			}
		}

		public virtual void SetReduceProgress(float reduceProgress)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetReduceProgress((reduceProgress));
			}
		}

		public virtual float GetCleanupProgress()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetCleanupProgress());
			}
		}

		public virtual void SetCleanupProgress(float cleanupProgress)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetCleanupProgress((cleanupProgress));
			}
		}

		public virtual float GetSetupProgress()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetSetupProgress());
			}
		}

		public virtual void SetSetupProgress(float setupProgress)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetSetupProgress((setupProgress));
			}
		}

		public virtual long GetSubmitTime()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetSubmitTime());
			}
		}

		public virtual void SetSubmitTime(long submitTime)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetSubmitTime((submitTime));
			}
		}

		public virtual long GetStartTime()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetStartTime());
			}
		}

		public virtual void SetStartTime(long startTime)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetStartTime((startTime));
			}
		}

		public virtual long GetFinishTime()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetFinishTime());
			}
		}

		public virtual void SetFinishTime(long finishTime)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetFinishTime((finishTime));
			}
		}

		public virtual string GetUser()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetUser());
			}
		}

		public virtual void SetUser(string user)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetUser((user));
			}
		}

		public virtual string GetJobName()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetJobName());
			}
		}

		public virtual void SetJobName(string jobName)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetJobName((jobName));
			}
		}

		public virtual string GetTrackingUrl()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetTrackingUrl());
			}
		}

		public virtual void SetTrackingUrl(string trackingUrl)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetTrackingUrl(trackingUrl);
			}
		}

		public virtual string GetDiagnostics()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return p.GetDiagnostics();
			}
		}

		public virtual void SetDiagnostics(string diagnostics)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetDiagnostics(diagnostics);
			}
		}

		public virtual string GetJobFile()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return p.GetJobFile();
			}
		}

		public virtual void SetJobFile(string jobFile)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetJobFile(jobFile);
			}
		}

		public virtual IList<AMInfo> GetAMInfos()
		{
			lock (this)
			{
				InitAMInfos();
				return this.amInfos;
			}
		}

		public virtual void SetAMInfos(IList<AMInfo> amInfos)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (amInfos == null)
				{
					this.builder.ClearAmInfos();
					this.amInfos = null;
					return;
				}
				InitAMInfos();
				this.amInfos.Clear();
				Sharpen.Collections.AddAll(this.amInfos, amInfos);
			}
		}

		private void InitAMInfos()
		{
			lock (this)
			{
				if (this.amInfos != null)
				{
					return;
				}
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				IList<MRProtos.AMInfoProto> list = p.GetAmInfosList();
				this.amInfos = new AList<AMInfo>();
				foreach (MRProtos.AMInfoProto amInfoProto in list)
				{
					this.amInfos.AddItem(ConvertFromProtoFormat(amInfoProto));
				}
			}
		}

		private void AddAMInfosToProto()
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.ClearAmInfos();
				if (this.amInfos == null)
				{
					return;
				}
				foreach (AMInfo amInfo in this.amInfos)
				{
					builder.AddAmInfos(ConvertToProtoFormat(amInfo));
				}
			}
		}

		private AMInfoPBImpl ConvertFromProtoFormat(MRProtos.AMInfoProto p)
		{
			return new AMInfoPBImpl(p);
		}

		private MRProtos.AMInfoProto ConvertToProtoFormat(AMInfo t)
		{
			return ((AMInfoPBImpl)t).GetProto();
		}

		private JobIdPBImpl ConvertFromProtoFormat(MRProtos.JobIdProto p)
		{
			return new JobIdPBImpl(p);
		}

		private MRProtos.JobIdProto ConvertToProtoFormat(JobId t)
		{
			return ((JobIdPBImpl)t).GetProto();
		}

		private MRProtos.JobStateProto ConvertToProtoFormat(JobState e)
		{
			return MRProtoUtils.ConvertToProtoFormat(e);
		}

		private JobState ConvertFromProtoFormat(MRProtos.JobStateProto e)
		{
			return MRProtoUtils.ConvertFromProtoFormat(e);
		}

		public virtual bool IsUber()
		{
			lock (this)
			{
				MRProtos.JobReportProtoOrBuilder p = viaProto ? proto : builder;
				return p.GetIsUber();
			}
		}

		public virtual void SetIsUber(bool isUber)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetIsUber(isUber);
			}
		}
	}
}
