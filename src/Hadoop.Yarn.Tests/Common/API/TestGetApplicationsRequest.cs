using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Lang.Math;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	public class TestGetApplicationsRequest
	{
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationsRequest()
		{
			GetApplicationsRequest request = GetApplicationsRequest.NewInstance();
			EnumSet<YarnApplicationState> appStates = EnumSet.Of(YarnApplicationState.Accepted
				);
			request.SetApplicationStates(appStates);
			ICollection<string> tags = new HashSet<string>();
			tags.AddItem("tag1");
			request.SetApplicationTags(tags);
			ICollection<string> types = new HashSet<string>();
			types.AddItem("type1");
			request.SetApplicationTypes(types);
			long startBegin = Runtime.CurrentTimeMillis();
			long startEnd = Runtime.CurrentTimeMillis() + 1;
			request.SetStartRange(startBegin, startEnd);
			long finishBegin = Runtime.CurrentTimeMillis() + 2;
			long finishEnd = Runtime.CurrentTimeMillis() + 3;
			request.SetFinishRange(finishBegin, finishEnd);
			long limit = 100L;
			request.SetLimit(limit);
			ICollection<string> queues = new HashSet<string>();
			queues.AddItem("queue1");
			request.SetQueues(queues);
			ICollection<string> users = new HashSet<string>();
			users.AddItem("user1");
			request.SetUsers(users);
			ApplicationsRequestScope scope = ApplicationsRequestScope.All;
			request.SetScope(scope);
			GetApplicationsRequest requestFromProto = new GetApplicationsRequestPBImpl(((GetApplicationsRequestPBImpl
				)request).GetProto());
			// verify the whole record equals with original record
			NUnit.Framework.Assert.AreEqual(requestFromProto, request);
			// verify all properties are the same as original request
			NUnit.Framework.Assert.AreEqual("ApplicationStates from proto is not the same with original request"
				, requestFromProto.GetApplicationStates(), appStates);
			NUnit.Framework.Assert.AreEqual("ApplicationTags from proto is not the same with original request"
				, requestFromProto.GetApplicationTags(), tags);
			NUnit.Framework.Assert.AreEqual("ApplicationTypes from proto is not the same with original request"
				, requestFromProto.GetApplicationTypes(), types);
			NUnit.Framework.Assert.AreEqual("StartRange from proto is not the same with original request"
				, requestFromProto.GetStartRange(), new LongRange(startBegin, startEnd));
			NUnit.Framework.Assert.AreEqual("FinishRange from proto is not the same with original request"
				, requestFromProto.GetFinishRange(), new LongRange(finishBegin, finishEnd));
			NUnit.Framework.Assert.AreEqual("Limit from proto is not the same with original request"
				, requestFromProto.GetLimit(), limit);
			NUnit.Framework.Assert.AreEqual("Queues from proto is not the same with original request"
				, requestFromProto.GetQueues(), queues);
			NUnit.Framework.Assert.AreEqual("Users from proto is not the same with original request"
				, requestFromProto.GetUsers(), users);
		}
	}
}
