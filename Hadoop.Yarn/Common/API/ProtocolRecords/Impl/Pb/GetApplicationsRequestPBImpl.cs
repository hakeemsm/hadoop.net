using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Commons.Lang.Math;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetApplicationsRequestPBImpl : GetApplicationsRequest
	{
		internal YarnServiceProtos.GetApplicationsRequestProto proto = YarnServiceProtos.GetApplicationsRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetApplicationsRequestProto.Builder builder = null;

		internal bool viaProto = false;

		internal ICollection<string> applicationTypes = null;

		internal EnumSet<YarnApplicationState> applicationStates = null;

		internal ICollection<string> users = null;

		internal ICollection<string> queues = null;

		internal long limit = long.MaxValue;

		internal LongRange start = null;

		internal LongRange finish = null;

		private ICollection<string> applicationTags;

		private ApplicationsRequestScope scope;

		public GetApplicationsRequestPBImpl()
		{
			builder = YarnServiceProtos.GetApplicationsRequestProto.NewBuilder();
		}

		public GetApplicationsRequestPBImpl(YarnServiceProtos.GetApplicationsRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetApplicationsRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetApplicationsRequestProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetApplicationsRequestProto)builder.Build());
			viaProto = true;
		}

		private void MergeLocalToBuilder()
		{
			if (applicationTypes != null && !applicationTypes.IsEmpty())
			{
				builder.ClearApplicationTypes();
				builder.AddAllApplicationTypes(applicationTypes);
			}
			if (applicationStates != null && !applicationStates.IsEmpty())
			{
				builder.ClearApplicationStates();
				builder.AddAllApplicationStates(Iterables.Transform(applicationStates, new _Function_91
					()));
			}
			if (applicationTags != null && !applicationTags.IsEmpty())
			{
				builder.ClearApplicationTags();
				builder.AddAllApplicationTags(this.applicationTags);
			}
			if (scope != null)
			{
				builder.SetScope(ProtoUtils.ConvertToProtoFormat(scope));
			}
			if (start != null)
			{
				builder.SetStartBegin(start.GetMinimumLong());
				builder.SetStartEnd(start.GetMaximumLong());
			}
			if (finish != null)
			{
				builder.SetFinishBegin(finish.GetMinimumLong());
				builder.SetFinishEnd(finish.GetMaximumLong());
			}
			if (limit != long.MaxValue)
			{
				builder.SetLimit(limit);
			}
			if (users != null && !users.IsEmpty())
			{
				builder.ClearUsers();
				builder.AddAllUsers(users);
			}
			if (queues != null && !queues.IsEmpty())
			{
				builder.ClearQueues();
				builder.AddAllQueues(queues);
			}
		}

		private sealed class _Function_91 : Function<YarnApplicationState, YarnProtos.YarnApplicationStateProto
			>
		{
			public _Function_91()
			{
			}

			public YarnProtos.YarnApplicationStateProto Apply(YarnApplicationState input)
			{
				return ProtoUtils.ConvertToProtoFormat(input);
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetApplicationsRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void InitApplicationTypes()
		{
			if (this.applicationTypes != null)
			{
				return;
			}
			YarnServiceProtos.GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<string> appTypeList = p.GetApplicationTypesList();
			this.applicationTypes = new HashSet<string>();
			Sharpen.Collections.AddAll(this.applicationTypes, appTypeList);
		}

		private void InitApplicationStates()
		{
			if (this.applicationStates != null)
			{
				return;
			}
			YarnServiceProtos.GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.YarnApplicationStateProto> appStatesList = p.GetApplicationStatesList
				();
			this.applicationStates = EnumSet.NoneOf<YarnApplicationState>();
			foreach (YarnProtos.YarnApplicationStateProto c in appStatesList)
			{
				this.applicationStates.AddItem(ProtoUtils.ConvertFromProtoFormat(c));
			}
		}

		private void InitUsers()
		{
			if (this.users != null)
			{
				return;
			}
			YarnServiceProtos.GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<string> usersList = p.GetUsersList();
			this.users = new HashSet<string>();
			Sharpen.Collections.AddAll(this.users, usersList);
		}

		private void InitQueues()
		{
			if (this.queues != null)
			{
				return;
			}
			YarnServiceProtos.GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<string> queuesList = p.GetQueuesList();
			this.queues = new HashSet<string>();
			Sharpen.Collections.AddAll(this.queues, queuesList);
		}

		public override ICollection<string> GetApplicationTypes()
		{
			InitApplicationTypes();
			return this.applicationTypes;
		}

		public override void SetApplicationTypes(ICollection<string> applicationTypes)
		{
			MaybeInitBuilder();
			if (applicationTypes == null)
			{
				builder.ClearApplicationTypes();
			}
			this.applicationTypes = applicationTypes;
		}

		private void InitApplicationTags()
		{
			if (this.applicationTags != null)
			{
				return;
			}
			YarnServiceProtos.GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
			this.applicationTags = new HashSet<string>();
			Sharpen.Collections.AddAll(this.applicationTags, p.GetApplicationTagsList());
		}

		public override ICollection<string> GetApplicationTags()
		{
			InitApplicationTags();
			return this.applicationTags;
		}

		public override void SetApplicationTags(ICollection<string> tags)
		{
			MaybeInitBuilder();
			if (tags == null || tags.IsEmpty())
			{
				builder.ClearApplicationTags();
				this.applicationTags = null;
				return;
			}
			// Convert applicationTags to lower case and add
			this.applicationTags = new HashSet<string>();
			foreach (string tag in tags)
			{
				this.applicationTags.AddItem(StringUtils.ToLowerCase(tag));
			}
		}

		public override EnumSet<YarnApplicationState> GetApplicationStates()
		{
			InitApplicationStates();
			return this.applicationStates;
		}

		private void InitScope()
		{
			if (this.scope != null)
			{
				return;
			}
			YarnServiceProtos.GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
			this.scope = ProtoUtils.ConvertFromProtoFormat(p.GetScope());
		}

		public override ApplicationsRequestScope GetScope()
		{
			InitScope();
			return this.scope;
		}

		public override void SetScope(ApplicationsRequestScope scope)
		{
			MaybeInitBuilder();
			if (scope == null)
			{
				builder.ClearScope();
			}
			this.scope = scope;
		}

		public override void SetApplicationStates(EnumSet<YarnApplicationState> applicationStates
			)
		{
			MaybeInitBuilder();
			if (applicationStates == null)
			{
				builder.ClearApplicationStates();
			}
			this.applicationStates = applicationStates;
		}

		public override void SetApplicationStates(ICollection<string> applicationStates)
		{
			EnumSet<YarnApplicationState> appStates = null;
			foreach (YarnApplicationState state in YarnApplicationState.Values())
			{
				if (applicationStates.Contains(StringUtils.ToLowerCase(state.ToString())))
				{
					if (appStates == null)
					{
						appStates = EnumSet.Of(state);
					}
					else
					{
						appStates.AddItem(state);
					}
				}
			}
			SetApplicationStates(appStates);
		}

		public override ICollection<string> GetUsers()
		{
			InitUsers();
			return this.users;
		}

		public override void SetUsers(ICollection<string> users)
		{
			MaybeInitBuilder();
			if (users == null)
			{
				builder.ClearUsers();
			}
			this.users = users;
		}

		public override ICollection<string> GetQueues()
		{
			InitQueues();
			return this.queues;
		}

		public override void SetQueues(ICollection<string> queues)
		{
			MaybeInitBuilder();
			if (queues == null)
			{
				builder.ClearQueues();
			}
			this.queues = queues;
		}

		public override long GetLimit()
		{
			if (this.limit == long.MaxValue)
			{
				YarnServiceProtos.GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
				this.limit = p.HasLimit() ? p.GetLimit() : long.MaxValue;
			}
			return this.limit;
		}

		public override void SetLimit(long limit)
		{
			MaybeInitBuilder();
			this.limit = limit;
		}

		public override LongRange GetStartRange()
		{
			if (this.start == null)
			{
				YarnServiceProtos.GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
				if (p.HasStartBegin() || p.HasStartEnd())
				{
					long begin = p.HasStartBegin() ? p.GetStartBegin() : 0L;
					long end = p.HasStartEnd() ? p.GetStartEnd() : long.MaxValue;
					this.start = new LongRange(begin, end);
				}
			}
			return this.start;
		}

		public override void SetStartRange(LongRange range)
		{
			this.start = range;
		}

		/// <exception cref="System.ArgumentException"/>
		public override void SetStartRange(long begin, long end)
		{
			if (begin > end)
			{
				throw new ArgumentException("begin > end in range (begin, " + "end): (" + begin +
					 ", " + end + ")");
			}
			this.start = new LongRange(begin, end);
		}

		public override LongRange GetFinishRange()
		{
			if (this.finish == null)
			{
				YarnServiceProtos.GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
				if (p.HasFinishBegin() || p.HasFinishEnd())
				{
					long begin = p.HasFinishBegin() ? p.GetFinishBegin() : 0L;
					long end = p.HasFinishEnd() ? p.GetFinishEnd() : long.MaxValue;
					this.finish = new LongRange(begin, end);
				}
			}
			return this.finish;
		}

		public override void SetFinishRange(LongRange range)
		{
			this.finish = range;
		}

		public override void SetFinishRange(long begin, long end)
		{
			if (begin > end)
			{
				throw new ArgumentException("begin > end in range (begin, " + "end): (" + begin +
					 ", " + end + ")");
			}
			this.finish = new LongRange(begin, end);
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
	}
}
