using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetApplicationsResponsePBImpl : GetApplicationsResponse
	{
		internal YarnServiceProtos.GetApplicationsResponseProto proto = YarnServiceProtos.GetApplicationsResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetApplicationsResponseProto.Builder builder = null;

		internal bool viaProto = false;

		internal IList<ApplicationReport> applicationList;

		public GetApplicationsResponsePBImpl()
		{
			builder = YarnServiceProtos.GetApplicationsResponseProto.NewBuilder();
		}

		public GetApplicationsResponsePBImpl(YarnServiceProtos.GetApplicationsResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override IList<ApplicationReport> GetApplicationList()
		{
			InitLocalApplicationsList();
			return this.applicationList;
		}

		public override void SetApplicationList(IList<ApplicationReport> applications)
		{
			MaybeInitBuilder();
			if (applications == null)
			{
				builder.ClearApplications();
			}
			this.applicationList = applications;
		}

		public virtual YarnServiceProtos.GetApplicationsResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetApplicationsResponseProto)builder
				.Build());
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
			if (this.applicationList != null)
			{
				AddLocalApplicationsToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetApplicationsResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetApplicationsResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		// Once this is called. containerList will never be null - until a getProto
		// is called.
		private void InitLocalApplicationsList()
		{
			if (this.applicationList != null)
			{
				return;
			}
			YarnServiceProtos.GetApplicationsResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.ApplicationReportProto> list = p.GetApplicationsList();
			applicationList = new AList<ApplicationReport>();
			foreach (YarnProtos.ApplicationReportProto a in list)
			{
				applicationList.AddItem(ConvertFromProtoFormat(a));
			}
		}

		private void AddLocalApplicationsToProto()
		{
			MaybeInitBuilder();
			builder.ClearApplications();
			if (applicationList == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ApplicationReportProto> iterable = new _IEnumerable_139(this
				);
			builder.AddAllApplications(iterable);
		}

		private sealed class _IEnumerable_139 : IEnumerable<YarnProtos.ApplicationReportProto
			>
		{
			public _IEnumerable_139(GetApplicationsResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ApplicationReportProto> GetEnumerator()
			{
				return new _IEnumerator_142(this);
			}

			private sealed class _IEnumerator_142 : IEnumerator<YarnProtos.ApplicationReportProto
				>
			{
				public _IEnumerator_142(_IEnumerable_139 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.applicationList.GetEnumerator();
				}

				internal IEnumerator<ApplicationReport> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ApplicationReportProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_139 _enclosing;
			}

			private readonly GetApplicationsResponsePBImpl _enclosing;
		}

		private ApplicationReportPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationReportProto
			 p)
		{
			return new ApplicationReportPBImpl(p);
		}

		private YarnProtos.ApplicationReportProto ConvertToProtoFormat(ApplicationReport 
			t)
		{
			return ((ApplicationReportPBImpl)t).GetProto();
		}
	}
}
