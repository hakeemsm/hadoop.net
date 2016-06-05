using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class QueueUserACLInfoPBImpl : QueueUserACLInfo
	{
		internal YarnProtos.QueueUserACLInfoProto proto = YarnProtos.QueueUserACLInfoProto
			.GetDefaultInstance();

		internal YarnProtos.QueueUserACLInfoProto.Builder builder = null;

		internal bool viaProto = false;

		internal IList<QueueACL> userAclsList;

		public QueueUserACLInfoPBImpl()
		{
			builder = YarnProtos.QueueUserACLInfoProto.NewBuilder();
		}

		public QueueUserACLInfoPBImpl(YarnProtos.QueueUserACLInfoProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override string GetQueueName()
		{
			YarnProtos.QueueUserACLInfoProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasQueueName()) ? p.GetQueueName() : null;
		}

		public override IList<QueueACL> GetUserAcls()
		{
			InitLocalQueueUserAclsList();
			return this.userAclsList;
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

		public override void SetUserAcls(IList<QueueACL> userAclsList)
		{
			if (userAclsList == null)
			{
				builder.ClearUserAcls();
			}
			this.userAclsList = userAclsList;
		}

		public virtual YarnProtos.QueueUserACLInfoProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.QueueUserACLInfoProto)builder.Build());
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

		private void InitLocalQueueUserAclsList()
		{
			if (this.userAclsList != null)
			{
				return;
			}
			YarnProtos.QueueUserACLInfoProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.QueueACLProto> list = p.GetUserAclsList();
			userAclsList = new AList<QueueACL>();
			foreach (YarnProtos.QueueACLProto a in list)
			{
				userAclsList.AddItem(ConvertFromProtoFormat(a));
			}
		}

		private void AddQueueACLsToProto()
		{
			MaybeInitBuilder();
			builder.ClearUserAcls();
			if (userAclsList == null)
			{
				return;
			}
			IEnumerable<YarnProtos.QueueACLProto> iterable = new _IEnumerable_129(this);
			builder.AddAllUserAcls(iterable);
		}

		private sealed class _IEnumerable_129 : IEnumerable<YarnProtos.QueueACLProto>
		{
			public _IEnumerable_129(QueueUserACLInfoPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.QueueACLProto> GetEnumerator()
			{
				return new _IEnumerator_132(this);
			}

			private sealed class _IEnumerator_132 : IEnumerator<YarnProtos.QueueACLProto>
			{
				public _IEnumerator_132(_IEnumerable_129 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.userAclsList.GetEnumerator();
				}

				internal IEnumerator<QueueACL> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.QueueACLProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_129 _enclosing;
			}

			private readonly QueueUserACLInfoPBImpl _enclosing;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.QueueUserACLInfoProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void MergeLocalToBuilder()
		{
			if (this.userAclsList != null)
			{
				AddQueueACLsToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.QueueUserACLInfoProto)builder.Build());
			viaProto = true;
		}

		private QueueACL ConvertFromProtoFormat(YarnProtos.QueueACLProto q)
		{
			return ProtoUtils.ConvertFromProtoFormat(q);
		}

		private YarnProtos.QueueACLProto ConvertToProtoFormat(QueueACL queueAcl)
		{
			return ProtoUtils.ConvertToProtoFormat(queueAcl);
		}
	}
}
