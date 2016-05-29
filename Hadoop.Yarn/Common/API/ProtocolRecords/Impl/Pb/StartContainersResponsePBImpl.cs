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
	public class StartContainersResponsePBImpl : StartContainersResponse
	{
		internal YarnServiceProtos.StartContainersResponseProto proto = YarnServiceProtos.StartContainersResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.StartContainersResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private IDictionary<string, ByteBuffer> servicesMetaData = null;

		private IList<ContainerId> succeededContainers = null;

		private IDictionary<ContainerId, SerializedException> failedContainers = null;

		public StartContainersResponsePBImpl()
		{
			builder = YarnServiceProtos.StartContainersResponseProto.NewBuilder();
		}

		public StartContainersResponsePBImpl(YarnServiceProtos.StartContainersResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.StartContainersResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.StartContainersResponseProto)builder
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
			if (this.servicesMetaData != null)
			{
				AddServicesMetaDataToProto();
			}
			if (this.succeededContainers != null)
			{
				AddSucceededContainersToProto();
			}
			if (this.failedContainers != null)
			{
				AddFailedContainersToProto();
			}
		}

		protected internal ByteBuffer ConvertFromProtoFormat(ByteString byteString)
		{
			return ProtoUtils.ConvertFromProtoFormat(byteString);
		}

		protected internal ByteString ConvertToProtoFormat(ByteBuffer byteBuffer)
		{
			return ProtoUtils.ConvertToProtoFormat(byteBuffer);
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

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.StartContainersResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.StartContainersResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override IDictionary<string, ByteBuffer> GetAllServicesMetaData()
		{
			InitServicesMetaData();
			return this.servicesMetaData;
		}

		public override void SetAllServicesMetaData(IDictionary<string, ByteBuffer> servicesMetaData
			)
		{
			if (servicesMetaData == null)
			{
				return;
			}
			InitServicesMetaData();
			this.servicesMetaData.Clear();
			this.servicesMetaData.PutAll(servicesMetaData);
		}

		private void InitServicesMetaData()
		{
			if (this.servicesMetaData != null)
			{
				return;
			}
			YarnServiceProtos.StartContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.StringBytesMapProto> list = p.GetServicesMetaDataList();
			this.servicesMetaData = new Dictionary<string, ByteBuffer>();
			foreach (YarnProtos.StringBytesMapProto c in list)
			{
				this.servicesMetaData[c.GetKey()] = ConvertFromProtoFormat(c.GetValue());
			}
		}

		private void AddServicesMetaDataToProto()
		{
			MaybeInitBuilder();
			builder.ClearServicesMetaData();
			if (servicesMetaData == null)
			{
				return;
			}
			IEnumerable<YarnProtos.StringBytesMapProto> iterable = new _IEnumerable_183(this);
			builder.AddAllServicesMetaData(iterable);
		}

		private sealed class _IEnumerable_183 : IEnumerable<YarnProtos.StringBytesMapProto
			>
		{
			public _IEnumerable_183(StartContainersResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.StringBytesMapProto> GetEnumerator()
			{
				return new _IEnumerator_187(this);
			}

			private sealed class _IEnumerator_187 : IEnumerator<YarnProtos.StringBytesMapProto
				>
			{
				public _IEnumerator_187(_IEnumerable_183 _enclosing)
				{
					this._enclosing = _enclosing;
					this.keyIter = this._enclosing._enclosing.servicesMetaData.Keys.GetEnumerator();
				}

				internal IEnumerator<string> keyIter;

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				public override YarnProtos.StringBytesMapProto Next()
				{
					string key = this.keyIter.Next();
					return ((YarnProtos.StringBytesMapProto)YarnProtos.StringBytesMapProto.NewBuilder
						().SetKey(key).SetValue(this._enclosing._enclosing.ConvertToProtoFormat(this._enclosing
						._enclosing.servicesMetaData[key])).Build());
				}

				public override bool HasNext()
				{
					return this.keyIter.HasNext();
				}

				private readonly _IEnumerable_183 _enclosing;
			}

			private readonly StartContainersResponsePBImpl _enclosing;
		}

		private void AddFailedContainersToProto()
		{
			MaybeInitBuilder();
			builder.ClearFailedRequests();
			if (this.failedContainers == null)
			{
				return;
			}
			IList<YarnServiceProtos.ContainerExceptionMapProto> protoList = new AList<YarnServiceProtos.ContainerExceptionMapProto
				>();
			foreach (KeyValuePair<ContainerId, SerializedException> entry in this.failedContainers)
			{
				protoList.AddItem(((YarnServiceProtos.ContainerExceptionMapProto)YarnServiceProtos.ContainerExceptionMapProto
					.NewBuilder().SetContainerId(ConvertToProtoFormat(entry.Key)).SetException(ConvertToProtoFormat
					(entry.Value)).Build()));
			}
			builder.AddAllFailedRequests(protoList);
		}

		private void AddSucceededContainersToProto()
		{
			MaybeInitBuilder();
			builder.ClearSucceededRequests();
			if (this.succeededContainers == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ContainerIdProto> iterable = new _IEnumerable_237(this);
			builder.AddAllSucceededRequests(iterable);
		}

		private sealed class _IEnumerable_237 : IEnumerable<YarnProtos.ContainerIdProto>
		{
			public _IEnumerable_237(StartContainersResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ContainerIdProto> GetEnumerator()
			{
				return new _IEnumerator_240(this);
			}

			private sealed class _IEnumerator_240 : IEnumerator<YarnProtos.ContainerIdProto>
			{
				public _IEnumerator_240(_IEnumerable_237 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.succeededContainers.GetEnumerator();
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

				private readonly _IEnumerable_237 _enclosing;
			}

			private readonly StartContainersResponsePBImpl _enclosing;
		}

		private void InitSucceededContainers()
		{
			if (this.succeededContainers != null)
			{
				return;
			}
			YarnServiceProtos.StartContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.ContainerIdProto> list = p.GetSucceededRequestsList();
			this.succeededContainers = new AList<ContainerId>();
			foreach (YarnProtos.ContainerIdProto c in list)
			{
				this.succeededContainers.AddItem(ConvertFromProtoFormat(c));
			}
		}

		public override IList<ContainerId> GetSuccessfullyStartedContainers()
		{
			InitSucceededContainers();
			return this.succeededContainers;
		}

		public override void SetSuccessfullyStartedContainers(IList<ContainerId> succeededContainers
			)
		{
			MaybeInitBuilder();
			if (succeededContainers == null)
			{
				builder.ClearSucceededRequests();
			}
			this.succeededContainers = succeededContainers;
		}

		private void InitFailedContainers()
		{
			if (this.failedContainers != null)
			{
				return;
			}
			YarnServiceProtos.StartContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnServiceProtos.ContainerExceptionMapProto> protoList = p.GetFailedRequestsList
				();
			this.failedContainers = new Dictionary<ContainerId, SerializedException>();
			foreach (YarnServiceProtos.ContainerExceptionMapProto ce in protoList)
			{
				this.failedContainers[ConvertFromProtoFormat(ce.GetContainerId())] = ConvertFromProtoFormat
					(ce.GetException());
			}
		}

		public override IDictionary<ContainerId, SerializedException> GetFailedRequests()
		{
			InitFailedContainers();
			return this.failedContainers;
		}

		public override void SetFailedRequests(IDictionary<ContainerId, SerializedException
			> failedContainers)
		{
			MaybeInitBuilder();
			if (failedContainers == null)
			{
				builder.ClearFailedRequests();
			}
			this.failedContainers = failedContainers;
		}
	}
}
