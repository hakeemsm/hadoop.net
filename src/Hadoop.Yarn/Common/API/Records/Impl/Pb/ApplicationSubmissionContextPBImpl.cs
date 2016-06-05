using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ApplicationSubmissionContextPBImpl : ApplicationSubmissionContext
	{
		internal YarnProtos.ApplicationSubmissionContextProto proto = YarnProtos.ApplicationSubmissionContextProto
			.GetDefaultInstance();

		internal YarnProtos.ApplicationSubmissionContextProto.Builder builder = null;

		internal bool viaProto = false;

		private ApplicationId applicationId = null;

		private Priority priority = null;

		private ContainerLaunchContext amContainer = null;

		private Resource resource = null;

		private ICollection<string> applicationTags = null;

		private ResourceRequest amResourceRequest = null;

		private LogAggregationContext logAggregationContext = null;

		private ReservationId reservationId = null;

		public ApplicationSubmissionContextPBImpl()
		{
			builder = YarnProtos.ApplicationSubmissionContextProto.NewBuilder();
		}

		public ApplicationSubmissionContextPBImpl(YarnProtos.ApplicationSubmissionContextProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ApplicationSubmissionContextProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ApplicationSubmissionContextProto)builder
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
			if (this.applicationId != null)
			{
				builder.SetApplicationId(ConvertToProtoFormat(this.applicationId));
			}
			if (this.priority != null)
			{
				builder.SetPriority(ConvertToProtoFormat(this.priority));
			}
			if (this.amContainer != null)
			{
				builder.SetAmContainerSpec(ConvertToProtoFormat(this.amContainer));
			}
			if (this.resource != null && !((ResourcePBImpl)this.resource).GetProto().Equals(builder
				.GetResource()))
			{
				builder.SetResource(ConvertToProtoFormat(this.resource));
			}
			if (this.applicationTags != null && !this.applicationTags.IsEmpty())
			{
				builder.ClearApplicationTags();
				builder.AddAllApplicationTags(this.applicationTags);
			}
			if (this.amResourceRequest != null)
			{
				builder.SetAmContainerResourceRequest(ConvertToProtoFormat(this.amResourceRequest
					));
			}
			if (this.logAggregationContext != null)
			{
				builder.SetLogAggregationContext(ConvertToProtoFormat(this.logAggregationContext)
					);
			}
			if (this.reservationId != null)
			{
				builder.SetReservationId(ConvertToProtoFormat(this.reservationId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ApplicationSubmissionContextProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ApplicationSubmissionContextProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override Priority GetPriority()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			if (this.priority != null)
			{
				return this.priority;
			}
			if (!p.HasPriority())
			{
				return null;
			}
			this.priority = ConvertFromProtoFormat(p.GetPriority());
			return this.priority;
		}

		public override void SetPriority(Priority priority)
		{
			MaybeInitBuilder();
			if (priority == null)
			{
				builder.ClearPriority();
			}
			this.priority = priority;
		}

		public override ApplicationId GetApplicationId()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			if (this.applicationId != null)
			{
				return applicationId;
			}
			// Else via proto
			if (!p.HasApplicationId())
			{
				return null;
			}
			applicationId = ConvertFromProtoFormat(p.GetApplicationId());
			return applicationId;
		}

		public override void SetApplicationId(ApplicationId applicationId)
		{
			MaybeInitBuilder();
			if (applicationId == null)
			{
				builder.ClearApplicationId();
			}
			this.applicationId = applicationId;
		}

		public override string GetApplicationName()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasApplicationName())
			{
				return null;
			}
			return (p.GetApplicationName());
		}

		public override void SetApplicationName(string applicationName)
		{
			MaybeInitBuilder();
			if (applicationName == null)
			{
				builder.ClearApplicationName();
				return;
			}
			builder.SetApplicationName((applicationName));
		}

		public override string GetQueue()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasQueue())
			{
				return null;
			}
			return (p.GetQueue());
		}

		public override string GetApplicationType()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasApplicationType())
			{
				return null;
			}
			return (p.GetApplicationType());
		}

		private void InitApplicationTags()
		{
			if (this.applicationTags != null)
			{
				return;
			}
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			this.applicationTags = new HashSet<string>();
			Sharpen.Collections.AddAll(this.applicationTags, p.GetApplicationTagsList());
		}

		public override ICollection<string> GetApplicationTags()
		{
			InitApplicationTags();
			return this.applicationTags;
		}

		public override void SetQueue(string queue)
		{
			MaybeInitBuilder();
			if (queue == null)
			{
				builder.ClearQueue();
				return;
			}
			builder.SetQueue((queue));
		}

		public override void SetApplicationType(string applicationType)
		{
			MaybeInitBuilder();
			if (applicationType == null)
			{
				builder.ClearApplicationType();
				return;
			}
			builder.SetApplicationType((applicationType));
		}

		private void CheckTags(ICollection<string> tags)
		{
			if (tags.Count > YarnConfiguration.ApplicationMaxTags)
			{
				throw new ArgumentException("Too many applicationTags, a maximum of only " + YarnConfiguration
					.ApplicationMaxTags + " are allowed!");
			}
			foreach (string tag in tags)
			{
				if (tag.Length > YarnConfiguration.ApplicationMaxTagLength)
				{
					throw new ArgumentException("Tag " + tag + " is too long, " + "maximum allowed length of a tag is "
						 + YarnConfiguration.ApplicationMaxTagLength);
				}
				if (!CharMatcher.Ascii.MatchesAllOf(tag))
				{
					throw new ArgumentException("A tag can only have ASCII " + "characters! Invalid tag - "
						 + tag);
				}
			}
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
			CheckTags(tags);
			// Convert applicationTags to lower case and add
			this.applicationTags = new HashSet<string>();
			foreach (string tag in tags)
			{
				this.applicationTags.AddItem(StringUtils.ToLowerCase(tag));
			}
		}

		public override ContainerLaunchContext GetAMContainerSpec()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			if (this.amContainer != null)
			{
				return amContainer;
			}
			// Else via proto
			if (!p.HasAmContainerSpec())
			{
				return null;
			}
			amContainer = ConvertFromProtoFormat(p.GetAmContainerSpec());
			return amContainer;
		}

		public override void SetAMContainerSpec(ContainerLaunchContext amContainer)
		{
			MaybeInitBuilder();
			if (amContainer == null)
			{
				builder.ClearAmContainerSpec();
			}
			this.amContainer = amContainer;
		}

		public override bool GetUnmanagedAM()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetUnmanagedAm();
		}

		public override void SetUnmanagedAM(bool value)
		{
			MaybeInitBuilder();
			builder.SetUnmanagedAm(value);
		}

		public override bool GetCancelTokensWhenComplete()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			//There is a default so cancelTokens should never be null
			return p.GetCancelTokensWhenComplete();
		}

		public override void SetCancelTokensWhenComplete(bool cancel)
		{
			MaybeInitBuilder();
			builder.SetCancelTokensWhenComplete(cancel);
		}

		public override int GetMaxAppAttempts()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetMaxAppAttempts();
		}

		public override void SetMaxAppAttempts(int maxAppAttempts)
		{
			MaybeInitBuilder();
			builder.SetMaxAppAttempts(maxAppAttempts);
		}

		public override Resource GetResource()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			if (this.resource != null)
			{
				return this.resource;
			}
			if (!p.HasResource())
			{
				return null;
			}
			this.resource = ConvertFromProtoFormat(p.GetResource());
			return this.resource;
		}

		public override void SetResource(Resource resource)
		{
			MaybeInitBuilder();
			if (resource == null)
			{
				builder.ClearResource();
			}
			this.resource = resource;
		}

		public override ReservationId GetReservationID()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			if (reservationId != null)
			{
				return reservationId;
			}
			if (!p.HasReservationId())
			{
				return null;
			}
			reservationId = ConvertFromProtoFormat(p.GetReservationId());
			return reservationId;
		}

		public override void SetReservationID(ReservationId reservationID)
		{
			MaybeInitBuilder();
			if (reservationID == null)
			{
				builder.ClearReservationId();
				return;
			}
			this.reservationId = reservationID;
		}

		public override void SetKeepContainersAcrossApplicationAttempts(bool keepContainers
			)
		{
			MaybeInitBuilder();
			builder.SetKeepContainersAcrossApplicationAttempts(keepContainers);
		}

		public override bool GetKeepContainersAcrossApplicationAttempts()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetKeepContainersAcrossApplicationAttempts();
		}

		private PriorityPBImpl ConvertFromProtoFormat(YarnProtos.PriorityProto p)
		{
			return new PriorityPBImpl(p);
		}

		private YarnProtos.PriorityProto ConvertToProtoFormat(Priority t)
		{
			return ((PriorityPBImpl)t).GetProto();
		}

		private ResourceRequestPBImpl ConvertFromProtoFormat(YarnProtos.ResourceRequestProto
			 p)
		{
			return new ResourceRequestPBImpl(p);
		}

		private YarnProtos.ResourceRequestProto ConvertToProtoFormat(ResourceRequest t)
		{
			return ((ResourceRequestPBImpl)t).GetProto();
		}

		private ApplicationIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationIdProto 
			p)
		{
			return new ApplicationIdPBImpl(p);
		}

		private YarnProtos.ApplicationIdProto ConvertToProtoFormat(ApplicationId t)
		{
			return ((ApplicationIdPBImpl)t).GetProto();
		}

		private ContainerLaunchContextPBImpl ConvertFromProtoFormat(YarnProtos.ContainerLaunchContextProto
			 p)
		{
			return new ContainerLaunchContextPBImpl(p);
		}

		private YarnProtos.ContainerLaunchContextProto ConvertToProtoFormat(ContainerLaunchContext
			 t)
		{
			return ((ContainerLaunchContextPBImpl)t).GetProto();
		}

		private ResourcePBImpl ConvertFromProtoFormat(YarnProtos.ResourceProto p)
		{
			return new ResourcePBImpl(p);
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource t)
		{
			return ((ResourcePBImpl)t).GetProto();
		}

		public override string GetNodeLabelExpression()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasNodeLabelExpression())
			{
				return null;
			}
			return p.GetNodeLabelExpression();
		}

		public override void SetNodeLabelExpression(string labelExpression)
		{
			MaybeInitBuilder();
			if (labelExpression == null)
			{
				builder.ClearNodeLabelExpression();
				return;
			}
			builder.SetNodeLabelExpression(labelExpression);
		}

		public override ResourceRequest GetAMContainerResourceRequest()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			if (this.amResourceRequest != null)
			{
				return amResourceRequest;
			}
			// Else via proto
			if (!p.HasAmContainerResourceRequest())
			{
				return null;
			}
			amResourceRequest = ConvertFromProtoFormat(p.GetAmContainerResourceRequest());
			return amResourceRequest;
		}

		public override void SetAMContainerResourceRequest(ResourceRequest request)
		{
			MaybeInitBuilder();
			if (request == null)
			{
				builder.ClearAmContainerResourceRequest();
			}
			this.amResourceRequest = request;
		}

		public override long GetAttemptFailuresValidityInterval()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetAttemptFailuresValidityInterval();
		}

		public override void SetAttemptFailuresValidityInterval(long attemptFailuresValidityInterval
			)
		{
			MaybeInitBuilder();
			builder.SetAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
		}

		private LogAggregationContextPBImpl ConvertFromProtoFormat(YarnProtos.LogAggregationContextProto
			 p)
		{
			return new LogAggregationContextPBImpl(p);
		}

		private YarnProtos.LogAggregationContextProto ConvertToProtoFormat(LogAggregationContext
			 t)
		{
			return ((LogAggregationContextPBImpl)t).GetProto();
		}

		public override LogAggregationContext GetLogAggregationContext()
		{
			YarnProtos.ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
			if (this.logAggregationContext != null)
			{
				return this.logAggregationContext;
			}
			// Else via proto
			if (!p.HasLogAggregationContext())
			{
				return null;
			}
			logAggregationContext = ConvertFromProtoFormat(p.GetLogAggregationContext());
			return logAggregationContext;
		}

		public override void SetLogAggregationContext(LogAggregationContext logAggregationContext
			)
		{
			MaybeInitBuilder();
			if (logAggregationContext == null)
			{
				builder.ClearLogAggregationContext();
			}
			this.logAggregationContext = logAggregationContext;
		}

		private ReservationIdPBImpl ConvertFromProtoFormat(YarnProtos.ReservationIdProto 
			p)
		{
			return new ReservationIdPBImpl(p);
		}

		private YarnProtos.ReservationIdProto ConvertToProtoFormat(ReservationId t)
		{
			return ((ReservationIdPBImpl)t).GetProto();
		}
	}
}
