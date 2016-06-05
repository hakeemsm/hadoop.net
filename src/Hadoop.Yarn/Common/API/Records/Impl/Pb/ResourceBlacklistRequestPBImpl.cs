using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ResourceBlacklistRequestPBImpl : ResourceBlacklistRequest
	{
		internal YarnProtos.ResourceBlacklistRequestProto proto = null;

		internal YarnProtos.ResourceBlacklistRequestProto.Builder builder = null;

		internal bool viaProto = false;

		internal IList<string> blacklistAdditions = null;

		internal IList<string> blacklistRemovals = null;

		public ResourceBlacklistRequestPBImpl()
		{
			builder = YarnProtos.ResourceBlacklistRequestProto.NewBuilder();
		}

		public ResourceBlacklistRequestPBImpl(YarnProtos.ResourceBlacklistRequestProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ResourceBlacklistRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ResourceBlacklistRequestProto)builder.Build
				());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ResourceBlacklistRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ResourceBlacklistRequestProto)builder.Build());
			viaProto = true;
		}

		private void MergeLocalToBuilder()
		{
			if (this.blacklistAdditions != null)
			{
				AddBlacklistAdditionsToProto();
			}
			if (this.blacklistRemovals != null)
			{
				AddBlacklistRemovalsToProto();
			}
		}

		private void AddBlacklistAdditionsToProto()
		{
			MaybeInitBuilder();
			builder.ClearBlacklistAdditions();
			if (this.blacklistAdditions == null)
			{
				return;
			}
			builder.AddAllBlacklistAdditions(this.blacklistAdditions);
		}

		private void AddBlacklistRemovalsToProto()
		{
			MaybeInitBuilder();
			builder.ClearBlacklistRemovals();
			if (this.blacklistRemovals == null)
			{
				return;
			}
			builder.AddAllBlacklistRemovals(this.blacklistRemovals);
		}

		private void InitBlacklistAdditions()
		{
			if (this.blacklistAdditions != null)
			{
				return;
			}
			YarnProtos.ResourceBlacklistRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<string> list = p.GetBlacklistAdditionsList();
			this.blacklistAdditions = new AList<string>();
			Sharpen.Collections.AddAll(this.blacklistAdditions, list);
		}

		private void InitBlacklistRemovals()
		{
			if (this.blacklistRemovals != null)
			{
				return;
			}
			YarnProtos.ResourceBlacklistRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<string> list = p.GetBlacklistRemovalsList();
			this.blacklistRemovals = new AList<string>();
			Sharpen.Collections.AddAll(this.blacklistRemovals, list);
		}

		public override IList<string> GetBlacklistAdditions()
		{
			InitBlacklistAdditions();
			return this.blacklistAdditions;
		}

		public override void SetBlacklistAdditions(IList<string> resourceNames)
		{
			if (resourceNames == null || resourceNames.IsEmpty())
			{
				if (this.blacklistAdditions != null)
				{
					this.blacklistAdditions.Clear();
				}
				return;
			}
			InitBlacklistAdditions();
			this.blacklistAdditions.Clear();
			Sharpen.Collections.AddAll(this.blacklistAdditions, resourceNames);
		}

		public override IList<string> GetBlacklistRemovals()
		{
			InitBlacklistRemovals();
			return this.blacklistRemovals;
		}

		public override void SetBlacklistRemovals(IList<string> resourceNames)
		{
			if (resourceNames == null || resourceNames.IsEmpty())
			{
				if (this.blacklistRemovals != null)
				{
					this.blacklistRemovals.Clear();
				}
				return;
			}
			InitBlacklistRemovals();
			this.blacklistRemovals.Clear();
			Sharpen.Collections.AddAll(this.blacklistRemovals, resourceNames);
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
	}
}
