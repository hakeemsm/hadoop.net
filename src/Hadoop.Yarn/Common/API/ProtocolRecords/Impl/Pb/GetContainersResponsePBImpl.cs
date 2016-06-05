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
	public class GetContainersResponsePBImpl : GetContainersResponse
	{
		internal YarnServiceProtos.GetContainersResponseProto proto = YarnServiceProtos.GetContainersResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetContainersResponseProto.Builder builder = null;

		internal bool viaProto = false;

		internal IList<ContainerReport> containerList;

		public GetContainersResponsePBImpl()
		{
			builder = YarnServiceProtos.GetContainersResponseProto.NewBuilder();
		}

		public GetContainersResponsePBImpl(YarnServiceProtos.GetContainersResponseProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override IList<ContainerReport> GetContainerList()
		{
			InitLocalContainerList();
			return this.containerList;
		}

		public override void SetContainerList(IList<ContainerReport> containers)
		{
			MaybeInitBuilder();
			if (containers == null)
			{
				builder.ClearContainers();
			}
			this.containerList = containers;
		}

		public virtual YarnServiceProtos.GetContainersResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetContainersResponseProto)builder
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
			if (this.containerList != null)
			{
				AddLocalContainersToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetContainersResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetContainersResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		// Once this is called. containerList will never be null - until a getProto
		// is called.
		private void InitLocalContainerList()
		{
			if (this.containerList != null)
			{
				return;
			}
			YarnServiceProtos.GetContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.ContainerReportProto> list = p.GetContainersList();
			containerList = new AList<ContainerReport>();
			foreach (YarnProtos.ContainerReportProto c in list)
			{
				containerList.AddItem(ConvertFromProtoFormat(c));
			}
		}

		private void AddLocalContainersToProto()
		{
			MaybeInitBuilder();
			builder.ClearContainers();
			if (containerList == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ContainerReportProto> iterable = new _IEnumerable_143(this
				);
			builder.AddAllContainers(iterable);
		}

		private sealed class _IEnumerable_143 : IEnumerable<YarnProtos.ContainerReportProto
			>
		{
			public _IEnumerable_143(GetContainersResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ContainerReportProto> GetEnumerator()
			{
				return new _IEnumerator_146(this);
			}

			private sealed class _IEnumerator_146 : IEnumerator<YarnProtos.ContainerReportProto
				>
			{
				public _IEnumerator_146(_IEnumerable_143 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.containerList.GetEnumerator();
				}

				internal IEnumerator<ContainerReport> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ContainerReportProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_143 _enclosing;
			}

			private readonly GetContainersResponsePBImpl _enclosing;
		}

		private ContainerReportPBImpl ConvertFromProtoFormat(YarnProtos.ContainerReportProto
			 p)
		{
			return new ContainerReportPBImpl(p);
		}

		private YarnProtos.ContainerReportProto ConvertToProtoFormat(ContainerReport t)
		{
			return ((ContainerReportPBImpl)t).GetProto();
		}
	}
}
