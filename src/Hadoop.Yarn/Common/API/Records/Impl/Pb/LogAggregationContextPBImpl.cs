using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class LogAggregationContextPBImpl : LogAggregationContext
	{
		internal YarnProtos.LogAggregationContextProto proto = YarnProtos.LogAggregationContextProto
			.GetDefaultInstance();

		internal YarnProtos.LogAggregationContextProto.Builder builder = null;

		internal bool viaProto = false;

		public LogAggregationContextPBImpl()
		{
			builder = YarnProtos.LogAggregationContextProto.NewBuilder();
		}

		public LogAggregationContextPBImpl(YarnProtos.LogAggregationContextProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.LogAggregationContextProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.LogAggregationContextProto)builder.Build(
				));
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
			proto = ((YarnProtos.LogAggregationContextProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.LogAggregationContextProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override string GetIncludePattern()
		{
			YarnProtos.LogAggregationContextProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasIncludePattern())
			{
				return null;
			}
			return p.GetIncludePattern();
		}

		public override void SetIncludePattern(string includePattern)
		{
			MaybeInitBuilder();
			if (includePattern == null)
			{
				builder.ClearIncludePattern();
				return;
			}
			builder.SetIncludePattern(includePattern);
		}

		public override string GetExcludePattern()
		{
			YarnProtos.LogAggregationContextProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasExcludePattern())
			{
				return null;
			}
			return p.GetExcludePattern();
		}

		public override void SetExcludePattern(string excludePattern)
		{
			MaybeInitBuilder();
			if (excludePattern == null)
			{
				builder.ClearExcludePattern();
				return;
			}
			builder.SetExcludePattern(excludePattern);
		}

		public override string GetRolledLogsIncludePattern()
		{
			YarnProtos.LogAggregationContextProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasRolledLogsIncludePattern())
			{
				return null;
			}
			return p.GetRolledLogsIncludePattern();
		}

		public override void SetRolledLogsIncludePattern(string rolledLogsIncludePattern)
		{
			MaybeInitBuilder();
			if (rolledLogsIncludePattern == null)
			{
				builder.ClearRolledLogsIncludePattern();
				return;
			}
			builder.SetRolledLogsIncludePattern(rolledLogsIncludePattern);
		}

		public override string GetRolledLogsExcludePattern()
		{
			YarnProtos.LogAggregationContextProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasRolledLogsExcludePattern())
			{
				return null;
			}
			return p.GetRolledLogsExcludePattern();
		}

		public override void SetRolledLogsExcludePattern(string rolledLogsExcludePattern)
		{
			MaybeInitBuilder();
			if (rolledLogsExcludePattern == null)
			{
				builder.ClearRolledLogsExcludePattern();
				return;
			}
			builder.SetRolledLogsExcludePattern(rolledLogsExcludePattern);
		}
	}
}
