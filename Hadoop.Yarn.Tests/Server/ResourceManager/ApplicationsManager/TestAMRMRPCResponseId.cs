using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Applicationsmanager
{
	public class TestAMRMRPCResponseId
	{
		private MockRM rm;

		internal ApplicationMasterService amService = null;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			this.rm = new MockRM();
			rm.Start();
			amService = rm.GetApplicationMasterService();
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (rm != null)
			{
				this.rm.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		private AllocateResponse Allocate(ApplicationAttemptId attemptId, AllocateRequest
			 req)
		{
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(attemptId.ToString
				());
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = rm.GetRMContext
				().GetRMApps()[attemptId.GetApplicationId()].GetRMAppAttempt(attemptId).GetAMRMToken
				();
			ugi.AddTokenIdentifier(token.DecodeIdentifier());
			return ugi.DoAs(new _PrivilegedExceptionAction_67(this, req));
		}

		private sealed class _PrivilegedExceptionAction_67 : PrivilegedExceptionAction<AllocateResponse
			>
		{
			public _PrivilegedExceptionAction_67(TestAMRMRPCResponseId _enclosing, AllocateRequest
				 req)
			{
				this._enclosing = _enclosing;
				this.req = req;
			}

			/// <exception cref="System.Exception"/>
			public AllocateResponse Run()
			{
				return this._enclosing.amService.Allocate(req);
			}

			private readonly TestAMRMRPCResponseId _enclosing;

			private readonly AllocateRequest req;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestARRMResponseId()
		{
			MockNM nm1 = rm.RegisterNode("h1:1234", 5000);
			RMApp app = rm.SubmitApp(2000);
			// Trigger the scheduling so the AM gets 'launched'
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			am.RegisterAppAttempt();
			AllocateRequest allocateRequest = AllocateRequest.NewInstance(0, 0F, null, null, 
				null);
			AllocateResponse response = Allocate(attempt.GetAppAttemptId(), allocateRequest);
			NUnit.Framework.Assert.AreEqual(1, response.GetResponseId());
			NUnit.Framework.Assert.IsTrue(response.GetAMCommand() == null);
			allocateRequest = AllocateRequest.NewInstance(response.GetResponseId(), 0F, null, 
				null, null);
			response = Allocate(attempt.GetAppAttemptId(), allocateRequest);
			NUnit.Framework.Assert.AreEqual(2, response.GetResponseId());
			/* try resending */
			response = Allocate(attempt.GetAppAttemptId(), allocateRequest);
			NUnit.Framework.Assert.AreEqual(2, response.GetResponseId());
			allocateRequest = AllocateRequest.NewInstance(0, 0F, null, null, null);
			try
			{
				Allocate(attempt.GetAppAttemptId(), allocateRequest);
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e.InnerException is InvalidApplicationMasterRequestException
					);
			}
		}
	}
}
