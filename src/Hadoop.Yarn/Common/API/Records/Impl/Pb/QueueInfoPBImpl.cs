using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class QueueInfoPBImpl : QueueInfo
	{
		internal YarnProtos.QueueInfoProto proto = YarnProtos.QueueInfoProto.GetDefaultInstance
			();

		internal YarnProtos.QueueInfoProto.Builder builder = null;

		internal bool viaProto = false;

		internal IList<ApplicationReport> applicationsList;

		internal IList<QueueInfo> childQueuesList;

		internal ICollection<string> accessibleNodeLabels;

		public QueueInfoPBImpl()
		{
			builder = YarnProtos.QueueInfoProto.NewBuilder();
		}

		public QueueInfoPBImpl(YarnProtos.QueueInfoProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override IList<ApplicationReport> GetApplications()
		{
			InitLocalApplicationsList();
			return this.applicationsList;
		}

		public override float GetCapacity()
		{
			YarnProtos.QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasCapacity()) ? p.GetCapacity() : -1;
		}

		public override IList<QueueInfo> GetChildQueues()
		{
			InitLocalChildQueuesList();
			return this.childQueuesList;
		}

		public override float GetCurrentCapacity()
		{
			YarnProtos.QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasCurrentCapacity()) ? p.GetCurrentCapacity() : 0;
		}

		public override float GetMaximumCapacity()
		{
			YarnProtos.QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasMaximumCapacity()) ? p.GetMaximumCapacity() : -1;
		}

		public override string GetQueueName()
		{
			YarnProtos.QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasQueueName()) ? p.GetQueueName() : null;
		}

		public override QueueState GetQueueState()
		{
			YarnProtos.QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasState())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetState());
		}

		public override void SetApplications(IList<ApplicationReport> applications)
		{
			if (applications == null)
			{
				builder.ClearApplications();
			}
			this.applicationsList = applications;
		}

		public override void SetCapacity(float capacity)
		{
			MaybeInitBuilder();
			builder.SetCapacity(capacity);
		}

		public override void SetChildQueues(IList<QueueInfo> childQueues)
		{
			if (childQueues == null)
			{
				builder.ClearChildQueues();
			}
			this.childQueuesList = childQueues;
		}

		public override void SetCurrentCapacity(float currentCapacity)
		{
			MaybeInitBuilder();
			builder.SetCurrentCapacity(currentCapacity);
		}

		public override void SetMaximumCapacity(float maximumCapacity)
		{
			MaybeInitBuilder();
			builder.SetMaximumCapacity(maximumCapacity);
		}

		public override void SetQueueName(string queueName)
		{
			MaybeInitBuilder();
			if (queueName == null)
			{
				builder.ClearQueueName();
				return;
			}
			builder.SetQueueName(queueName);
		}

		public override void SetQueueState(QueueState queueState)
		{
			MaybeInitBuilder();
			if (queueState == null)
			{
				builder.ClearState();
				return;
			}
			builder.SetState(ConvertToProtoFormat(queueState));
		}

		public virtual YarnProtos.QueueInfoProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.QueueInfoProto)builder.Build());
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

		private void InitLocalApplicationsList()
		{
			if (this.applicationsList != null)
			{
				return;
			}
			YarnProtos.QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.ApplicationReportProto> list = p.GetApplicationsList();
			applicationsList = new AList<ApplicationReport>();
			foreach (YarnProtos.ApplicationReportProto a in list)
			{
				applicationsList.AddItem(ConvertFromProtoFormat(a));
			}
		}

		private void AddApplicationsToProto()
		{
			MaybeInitBuilder();
			builder.ClearApplications();
			if (applicationsList == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ApplicationReportProto> iterable = new _IEnumerable_204(this
				);
			builder.AddAllApplications(iterable);
		}

		private sealed class _IEnumerable_204 : IEnumerable<YarnProtos.ApplicationReportProto
			>
		{
			public _IEnumerable_204(QueueInfoPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ApplicationReportProto> GetEnumerator()
			{
				return new _IEnumerator_207(this);
			}

			private sealed class _IEnumerator_207 : IEnumerator<YarnProtos.ApplicationReportProto
				>
			{
				public _IEnumerator_207(_IEnumerable_204 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.applicationsList.GetEnumerator();
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

				private readonly _IEnumerable_204 _enclosing;
			}

			private readonly QueueInfoPBImpl _enclosing;
		}

		private void InitLocalChildQueuesList()
		{
			if (this.childQueuesList != null)
			{
				return;
			}
			YarnProtos.QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.QueueInfoProto> list = p.GetChildQueuesList();
			childQueuesList = new AList<QueueInfo>();
			foreach (YarnProtos.QueueInfoProto a in list)
			{
				childQueuesList.AddItem(ConvertFromProtoFormat(a));
			}
		}

		private void AddChildQueuesInfoToProto()
		{
			MaybeInitBuilder();
			builder.ClearChildQueues();
			if (childQueuesList == null)
			{
				return;
			}
			IEnumerable<YarnProtos.QueueInfoProto> iterable = new _IEnumerable_251(this);
			builder.AddAllChildQueues(iterable);
		}

		private sealed class _IEnumerable_251 : IEnumerable<YarnProtos.QueueInfoProto>
		{
			public _IEnumerable_251(QueueInfoPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.QueueInfoProto> GetEnumerator()
			{
				return new _IEnumerator_254(this);
			}

			private sealed class _IEnumerator_254 : IEnumerator<YarnProtos.QueueInfoProto>
			{
				public _IEnumerator_254(_IEnumerable_251 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.childQueuesList.GetEnumerator();
				}

				internal IEnumerator<QueueInfo> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.QueueInfoProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_251 _enclosing;
			}

			private readonly QueueInfoPBImpl _enclosing;
		}

		private void MergeLocalToBuilder()
		{
			if (this.childQueuesList != null)
			{
				AddChildQueuesInfoToProto();
			}
			if (this.applicationsList != null)
			{
				AddApplicationsToProto();
			}
			if (this.accessibleNodeLabels != null)
			{
				builder.ClearAccessibleNodeLabels();
				builder.AddAllAccessibleNodeLabels(this.accessibleNodeLabels);
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.QueueInfoProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.QueueInfoProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private ApplicationReportPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationReportProto
			 a)
		{
			return new ApplicationReportPBImpl(a);
		}

		private YarnProtos.ApplicationReportProto ConvertToProtoFormat(ApplicationReport 
			t)
		{
			return ((ApplicationReportPBImpl)t).GetProto();
		}

		private Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB.QueueInfoPBImpl ConvertFromProtoFormat
			(YarnProtos.QueueInfoProto a)
		{
			return new Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB.QueueInfoPBImpl(a);
		}

		private YarnProtos.QueueInfoProto ConvertToProtoFormat(QueueInfo q)
		{
			return ((Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB.QueueInfoPBImpl)q).GetProto();
		}

		private QueueState ConvertFromProtoFormat(YarnProtos.QueueStateProto q)
		{
			return ProtoUtils.ConvertFromProtoFormat(q);
		}

		private YarnProtos.QueueStateProto ConvertToProtoFormat(QueueState queueState)
		{
			return ProtoUtils.ConvertToProtoFormat(queueState);
		}

		public override void SetAccessibleNodeLabels(ICollection<string> nodeLabels)
		{
			MaybeInitBuilder();
			builder.ClearAccessibleNodeLabels();
			this.accessibleNodeLabels = nodeLabels;
		}

		private void InitNodeLabels()
		{
			if (this.accessibleNodeLabels != null)
			{
				return;
			}
			YarnProtos.QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
			this.accessibleNodeLabels = new HashSet<string>();
			Sharpen.Collections.AddAll(this.accessibleNodeLabels, p.GetAccessibleNodeLabelsList
				());
		}

		public override ICollection<string> GetAccessibleNodeLabels()
		{
			InitNodeLabels();
			return this.accessibleNodeLabels;
		}

		public override string GetDefaultNodeLabelExpression()
		{
			YarnProtos.QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasDefaultNodeLabelExpression()) ? p.GetDefaultNodeLabelExpression().Trim
				() : null;
		}

		public override void SetDefaultNodeLabelExpression(string defaultNodeLabelExpression
			)
		{
			MaybeInitBuilder();
			if (defaultNodeLabelExpression == null)
			{
				builder.ClearDefaultNodeLabelExpression();
				return;
			}
			builder.SetDefaultNodeLabelExpression(defaultNodeLabelExpression);
		}
	}
}
