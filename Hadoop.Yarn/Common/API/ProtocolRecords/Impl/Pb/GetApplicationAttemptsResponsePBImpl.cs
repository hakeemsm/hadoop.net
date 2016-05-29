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
	public class GetApplicationAttemptsResponsePBImpl : GetApplicationAttemptsResponse
	{
		internal YarnServiceProtos.GetApplicationAttemptsResponseProto proto = YarnServiceProtos.GetApplicationAttemptsResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetApplicationAttemptsResponseProto.Builder builder = 
			null;

		internal bool viaProto = false;

		internal IList<ApplicationAttemptReport> applicationAttemptList;

		public GetApplicationAttemptsResponsePBImpl()
		{
			builder = YarnServiceProtos.GetApplicationAttemptsResponseProto.NewBuilder();
		}

		public GetApplicationAttemptsResponsePBImpl(YarnServiceProtos.GetApplicationAttemptsResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override IList<ApplicationAttemptReport> GetApplicationAttemptList()
		{
			InitLocalApplicationAttemptsList();
			return this.applicationAttemptList;
		}

		public override void SetApplicationAttemptList(IList<ApplicationAttemptReport> applicationAttempts
			)
		{
			MaybeInitBuilder();
			if (applicationAttempts == null)
			{
				builder.ClearApplicationAttempts();
			}
			this.applicationAttemptList = applicationAttempts;
		}

		public virtual YarnServiceProtos.GetApplicationAttemptsResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetApplicationAttemptsResponseProto
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
			if (this.applicationAttemptList != null)
			{
				AddLocalApplicationAttemptsToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetApplicationAttemptsResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetApplicationAttemptsResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		// Once this is called. containerList will never be null - until a getProto
		// is called.
		private void InitLocalApplicationAttemptsList()
		{
			if (this.applicationAttemptList != null)
			{
				return;
			}
			YarnServiceProtos.GetApplicationAttemptsResponseProtoOrBuilder p = viaProto ? proto
				 : builder;
			IList<YarnProtos.ApplicationAttemptReportProto> list = p.GetApplicationAttemptsList
				();
			applicationAttemptList = new AList<ApplicationAttemptReport>();
			foreach (YarnProtos.ApplicationAttemptReportProto a in list)
			{
				applicationAttemptList.AddItem(ConvertFromProtoFormat(a));
			}
		}

		private void AddLocalApplicationAttemptsToProto()
		{
			MaybeInitBuilder();
			builder.ClearApplicationAttempts();
			if (applicationAttemptList == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ApplicationAttemptReportProto> iterable = new _IEnumerable_146
				(this);
			builder.AddAllApplicationAttempts(iterable);
		}

		private sealed class _IEnumerable_146 : IEnumerable<YarnProtos.ApplicationAttemptReportProto
			>
		{
			public _IEnumerable_146(GetApplicationAttemptsResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ApplicationAttemptReportProto> GetEnumerator
				()
			{
				return new _IEnumerator_149(this);
			}

			private sealed class _IEnumerator_149 : IEnumerator<YarnProtos.ApplicationAttemptReportProto
				>
			{
				public _IEnumerator_149(_IEnumerable_146 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.applicationAttemptList.GetEnumerator();
				}

				internal IEnumerator<ApplicationAttemptReport> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ApplicationAttemptReportProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_146 _enclosing;
			}

			private readonly GetApplicationAttemptsResponsePBImpl _enclosing;
		}

		private ApplicationAttemptReportPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationAttemptReportProto
			 p)
		{
			return new ApplicationAttemptReportPBImpl(p);
		}

		private YarnProtos.ApplicationAttemptReportProto ConvertToProtoFormat(ApplicationAttemptReport
			 t)
		{
			return ((ApplicationAttemptReportPBImpl)t).GetProto();
		}
	}
}
