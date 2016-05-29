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
	public class StopContainersResponsePBImpl : StopContainersResponse
	{
		internal YarnServiceProtos.StopContainersResponseProto proto = YarnServiceProtos.StopContainersResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.StopContainersResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private IList<ContainerId> succeededRequests = null;

		private IDictionary<ContainerId, SerializedException> failedRequests = null;

		public StopContainersResponsePBImpl()
		{
			builder = YarnServiceProtos.StopContainersResponseProto.NewBuilder();
		}

		public StopContainersResponsePBImpl(YarnServiceProtos.StopContainersResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.StopContainersResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.StopContainersResponseProto)builder
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

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.StopContainersResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.StopContainersResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void MergeLocalToBuilder()
		{
			if (this.succeededRequests != null)
			{
				AddSucceededRequestsToProto();
			}
			if (this.failedRequests != null)
			{
				AddFailedRequestsToProto();
			}
		}

		private void AddSucceededRequestsToProto()
		{
			MaybeInitBuilder();
			builder.ClearSucceededRequests();
			if (this.succeededRequests == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ContainerIdProto> iterable = new _IEnumerable_120(this);
			builder.AddAllSucceededRequests(iterable);
		}

		private sealed class _IEnumerable_120 : IEnumerable<YarnProtos.ContainerIdProto>
		{
			public _IEnumerable_120(StopContainersResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ContainerIdProto> GetEnumerator()
			{
				return new _IEnumerator_123(this);
			}

			private sealed class _IEnumerator_123 : IEnumerator<YarnProtos.ContainerIdProto>
			{
				public _IEnumerator_123(_IEnumerable_120 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.succeededRequests.GetEnumerator();
				}

				internal IEnumerator<ContainerId> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ContainerIdProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_120 _enclosing;
			}

			private readonly StopContainersResponsePBImpl _enclosing;
		}

		private void AddFailedRequestsToProto()
		{
			MaybeInitBuilder();
			builder.ClearFailedRequests();
			if (this.failedRequests == null)
			{
				return;
			}
			IList<YarnServiceProtos.ContainerExceptionMapProto> protoList = new AList<YarnServiceProtos.ContainerExceptionMapProto
				>();
			foreach (KeyValuePair<ContainerId, SerializedException> entry in this.failedRequests)
			{
				protoList.AddItem(((YarnServiceProtos.ContainerExceptionMapProto)YarnServiceProtos.ContainerExceptionMapProto
					.NewBuilder().SetContainerId(ConvertToProtoFormat(entry.Key)).SetException(ConvertToProtoFormat
					(entry.Value)).Build()));
			}
			builder.AddAllFailedRequests(protoList);
		}

		private void InitSucceededRequests()
		{
			if (this.succeededRequests != null)
			{
				return;
			}
			YarnServiceProtos.StopContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.ContainerIdProto> list = p.GetSucceededRequestsList();
			this.succeededRequests = new AList<ContainerId>();
			foreach (YarnProtos.ContainerIdProto c in list)
			{
				this.succeededRequests.AddItem(ConvertFromProtoFormat(c));
			}
		}

		private void InitFailedRequests()
		{
			if (this.failedRequests != null)
			{
				return;
			}
			YarnServiceProtos.StopContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnServiceProtos.ContainerExceptionMapProto> protoList = p.GetFailedRequestsList
				();
			this.failedRequests = new Dictionary<ContainerId, SerializedException>();
			foreach (YarnServiceProtos.ContainerExceptionMapProto ce in protoList)
			{
				this.failedRequests[ConvertFromProtoFormat(ce.GetContainerId())] = ConvertFromProtoFormat
					(ce.GetException());
			}
		}

		public override IList<ContainerId> GetSuccessfullyStoppedContainers()
		{
			InitSucceededRequests();
			return this.succeededRequests;
		}

		public override void SetSuccessfullyStoppedContainers(IList<ContainerId> succeededRequests
			)
		{
			MaybeInitBuilder();
			if (succeededRequests == null)
			{
				builder.ClearSucceededRequests();
			}
			this.succeededRequests = succeededRequests;
		}

		public override IDictionary<ContainerId, SerializedException> GetFailedRequests()
		{
			InitFailedRequests();
			return this.failedRequests;
		}

		public override void SetFailedRequests(IDictionary<ContainerId, SerializedException
			> failedRequests)
		{
			MaybeInitBuilder();
			if (failedRequests == null)
			{
				builder.ClearFailedRequests();
			}
			this.failedRequests = failedRequests;
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto p)
		{
			return new ContainerIdPBImpl(p);
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId t)
		{
			return ((ContainerIdPBImpl)t).GetProto();
		}

		private SerializedExceptionPBImpl ConvertFromProtoFormat(YarnProtos.SerializedExceptionProto
			 p)
		{
			return new SerializedExceptionPBImpl(p);
		}

		private YarnProtos.SerializedExceptionProto ConvertToProtoFormat(SerializedException
			 t)
		{
			return ((SerializedExceptionPBImpl)t).GetProto();
		}
	}
}
