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
	public class GetQueueUserAclsInfoResponsePBImpl : GetQueueUserAclsInfoResponse
	{
		internal IList<QueueUserACLInfo> queueUserAclsInfoList;

		internal YarnServiceProtos.GetQueueUserAclsInfoResponseProto proto = YarnServiceProtos.GetQueueUserAclsInfoResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetQueueUserAclsInfoResponseProto.Builder builder = null;

		internal bool viaProto = false;

		public GetQueueUserAclsInfoResponsePBImpl()
		{
			builder = YarnServiceProtos.GetQueueUserAclsInfoResponseProto.NewBuilder();
		}

		public GetQueueUserAclsInfoResponsePBImpl(YarnServiceProtos.GetQueueUserAclsInfoResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override IList<QueueUserACLInfo> GetUserAclsInfoList()
		{
			InitLocalQueueUserAclsList();
			return queueUserAclsInfoList;
		}

		public override void SetUserAclsInfoList(IList<QueueUserACLInfo> queueUserAclsList
			)
		{
			if (queueUserAclsList == null)
			{
				builder.ClearQueueUserAcls();
			}
			this.queueUserAclsInfoList = queueUserAclsList;
		}

		public virtual YarnServiceProtos.GetQueueUserAclsInfoResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetQueueUserAclsInfoResponseProto)
				builder.Build());
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
			if (this.queueUserAclsInfoList != null)
			{
				AddLocalQueueUserACLInfosToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetQueueUserAclsInfoResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetQueueUserAclsInfoResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		// Once this is called. containerList will never be null - until a getProto
		// is called.
		private void InitLocalQueueUserAclsList()
		{
			if (this.queueUserAclsInfoList != null)
			{
				return;
			}
			YarnServiceProtos.GetQueueUserAclsInfoResponseProtoOrBuilder p = viaProto ? proto
				 : builder;
			IList<YarnProtos.QueueUserACLInfoProto> list = p.GetQueueUserAclsList();
			queueUserAclsInfoList = new AList<QueueUserACLInfo>();
			foreach (YarnProtos.QueueUserACLInfoProto a in list)
			{
				queueUserAclsInfoList.AddItem(ConvertFromProtoFormat(a));
			}
		}

		private void AddLocalQueueUserACLInfosToProto()
		{
			MaybeInitBuilder();
			builder.ClearQueueUserAcls();
			if (queueUserAclsInfoList == null)
			{
				return;
			}
			IEnumerable<YarnProtos.QueueUserACLInfoProto> iterable = new _IEnumerable_139(this
				);
			builder.AddAllQueueUserAcls(iterable);
		}

		private sealed class _IEnumerable_139 : IEnumerable<YarnProtos.QueueUserACLInfoProto
			>
		{
			public _IEnumerable_139(GetQueueUserAclsInfoResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.QueueUserACLInfoProto> GetEnumerator()
			{
				return new _IEnumerator_142(this);
			}

			private sealed class _IEnumerator_142 : IEnumerator<YarnProtos.QueueUserACLInfoProto
				>
			{
				public _IEnumerator_142(_IEnumerable_139 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.queueUserAclsInfoList.GetEnumerator();
				}

				internal IEnumerator<QueueUserACLInfo> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.QueueUserACLInfoProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_139 _enclosing;
			}

			private readonly GetQueueUserAclsInfoResponsePBImpl _enclosing;
		}

		private QueueUserACLInfoPBImpl ConvertFromProtoFormat(YarnProtos.QueueUserACLInfoProto
			 p)
		{
			return new QueueUserACLInfoPBImpl(p);
		}

		private YarnProtos.QueueUserACLInfoProto ConvertToProtoFormat(QueueUserACLInfo t)
		{
			return ((QueueUserACLInfoPBImpl)t).GetProto();
		}
	}
}
