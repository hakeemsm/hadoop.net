using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class StartContainersRequestPBImpl : StartContainersRequest
	{
		internal YarnServiceProtos.StartContainersRequestProto proto = YarnServiceProtos.StartContainersRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.StartContainersRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private IList<StartContainerRequest> requests = null;

		public StartContainersRequestPBImpl()
		{
			builder = YarnServiceProtos.StartContainersRequestProto.NewBuilder();
		}

		public StartContainersRequestPBImpl(YarnServiceProtos.StartContainersRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.StartContainersRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.StartContainersRequestProto)builder
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

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.StartContainersRequestProto)builder.Build());
			viaProto = true;
		}

		private void MergeLocalToBuilder()
		{
			if (requests != null)
			{
				AddLocalRequestsToProto();
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.StartContainersRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void AddLocalRequestsToProto()
		{
			MaybeInitBuilder();
			builder.ClearStartContainerRequest();
			IList<YarnServiceProtos.StartContainerRequestProto> protoList = new AList<YarnServiceProtos.StartContainerRequestProto
				>();
			foreach (StartContainerRequest r in this.requests)
			{
				protoList.AddItem(ConvertToProtoFormat(r));
			}
			builder.AddAllStartContainerRequest(protoList);
		}

		private void InitLocalRequests()
		{
			YarnServiceProtos.StartContainersRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnServiceProtos.StartContainerRequestProto> requestList = p.GetStartContainerRequestList
				();
			this.requests = new AList<StartContainerRequest>();
			foreach (YarnServiceProtos.StartContainerRequestProto r in requestList)
			{
				this.requests.AddItem(ConvertFromProtoFormat(r));
			}
		}

		public override void SetStartContainerRequests(IList<StartContainerRequest> requests
			)
		{
			MaybeInitBuilder();
			if (requests == null)
			{
				builder.ClearStartContainerRequest();
			}
			this.requests = requests;
		}

		public override IList<StartContainerRequest> GetStartContainerRequests()
		{
			if (this.requests != null)
			{
				return this.requests;
			}
			InitLocalRequests();
			return this.requests;
		}

		private StartContainerRequestPBImpl ConvertFromProtoFormat(YarnServiceProtos.StartContainerRequestProto
			 p)
		{
			return new StartContainerRequestPBImpl(p);
		}

		private YarnServiceProtos.StartContainerRequestProto ConvertToProtoFormat(StartContainerRequest
			 t)
		{
			return ((StartContainerRequestPBImpl)t).GetProto();
		}
	}
}
