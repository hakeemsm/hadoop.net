using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	public class TestYARNTokenIdentifier
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNMTokenIdentifier()
		{
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(ApplicationId
				.NewInstance(1, 1), 1);
			NodeId nodeId = NodeId.NewInstance("host0", 0);
			string applicationSubmitter = "usr0";
			int masterKeyId = 1;
			NMTokenIdentifier token = new NMTokenIdentifier(appAttemptId, nodeId, applicationSubmitter
				, masterKeyId);
			NMTokenIdentifier anotherToken = new NMTokenIdentifier();
			byte[] tokenContent = token.GetBytes();
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(tokenContent, tokenContent.Length);
			anotherToken.ReadFields(dib);
			// verify the whole record equals with original record
			NUnit.Framework.Assert.AreEqual("Token is not the same after serialization " + "and deserialization."
				, token, anotherToken);
			// verify all properties are the same as original
			NUnit.Framework.Assert.AreEqual("appAttemptId from proto is not the same with original token"
				, anotherToken.GetApplicationAttemptId(), appAttemptId);
			NUnit.Framework.Assert.AreEqual("NodeId from proto is not the same with original token"
				, anotherToken.GetNodeId(), nodeId);
			NUnit.Framework.Assert.AreEqual("applicationSubmitter from proto is not the same with original token"
				, anotherToken.GetApplicationSubmitter(), applicationSubmitter);
			NUnit.Framework.Assert.AreEqual("masterKeyId from proto is not the same with original token"
				, anotherToken.GetKeyId(), masterKeyId);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAMRMTokenIdentifier()
		{
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(ApplicationId
				.NewInstance(1, 1), 1);
			int masterKeyId = 1;
			AMRMTokenIdentifier token = new AMRMTokenIdentifier(appAttemptId, masterKeyId);
			AMRMTokenIdentifier anotherToken = new AMRMTokenIdentifier();
			byte[] tokenContent = token.GetBytes();
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(tokenContent, tokenContent.Length);
			anotherToken.ReadFields(dib);
			// verify the whole record equals with original record
			NUnit.Framework.Assert.AreEqual("Token is not the same after serialization " + "and deserialization."
				, token, anotherToken);
			NUnit.Framework.Assert.AreEqual("ApplicationAttemptId from proto is not the same with original token"
				, anotherToken.GetApplicationAttemptId(), appAttemptId);
			NUnit.Framework.Assert.AreEqual("masterKeyId from proto is not the same with original token"
				, anotherToken.GetKeyId(), masterKeyId);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestClientToAMTokenIdentifier()
		{
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(ApplicationId
				.NewInstance(1, 1), 1);
			string clientName = "user";
			ClientToAMTokenIdentifier token = new ClientToAMTokenIdentifier(appAttemptId, clientName
				);
			ClientToAMTokenIdentifier anotherToken = new ClientToAMTokenIdentifier();
			byte[] tokenContent = token.GetBytes();
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(tokenContent, tokenContent.Length);
			anotherToken.ReadFields(dib);
			// verify the whole record equals with original record
			NUnit.Framework.Assert.AreEqual("Token is not the same after serialization " + "and deserialization."
				, token, anotherToken);
			NUnit.Framework.Assert.AreEqual("ApplicationAttemptId from proto is not the same with original token"
				, anotherToken.GetApplicationAttemptID(), appAttemptId);
			NUnit.Framework.Assert.AreEqual("clientName from proto is not the same with original token"
				, anotherToken.GetClientName(), clientName);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerTokenIdentifier()
		{
			ContainerId containerID = ContainerId.NewContainerId(ApplicationAttemptId.NewInstance
				(ApplicationId.NewInstance(1, 1), 1), 1);
			string hostName = "host0";
			string appSubmitter = "usr0";
			Resource r = Resource.NewInstance(1024, 1);
			long expiryTimeStamp = 1000;
			int masterKeyId = 1;
			long rmIdentifier = 1;
			Priority priority = Priority.NewInstance(1);
			long creationTime = 1000;
			ContainerTokenIdentifier token = new ContainerTokenIdentifier(containerID, hostName
				, appSubmitter, r, expiryTimeStamp, masterKeyId, rmIdentifier, priority, creationTime
				);
			ContainerTokenIdentifier anotherToken = new ContainerTokenIdentifier();
			byte[] tokenContent = token.GetBytes();
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(tokenContent, tokenContent.Length);
			anotherToken.ReadFields(dib);
			// verify the whole record equals with original record
			NUnit.Framework.Assert.AreEqual("Token is not the same after serialization " + "and deserialization."
				, token, anotherToken);
			NUnit.Framework.Assert.AreEqual("ContainerID from proto is not the same with original token"
				, anotherToken.GetContainerID(), containerID);
			NUnit.Framework.Assert.AreEqual("Hostname from proto is not the same with original token"
				, anotherToken.GetNmHostAddress(), hostName);
			NUnit.Framework.Assert.AreEqual("ApplicationSubmitter from proto is not the same with original token"
				, anotherToken.GetApplicationSubmitter(), appSubmitter);
			NUnit.Framework.Assert.AreEqual("Resource from proto is not the same with original token"
				, anotherToken.GetResource(), r);
			NUnit.Framework.Assert.AreEqual("expiryTimeStamp from proto is not the same with original token"
				, anotherToken.GetExpiryTimeStamp(), expiryTimeStamp);
			NUnit.Framework.Assert.AreEqual("KeyId from proto is not the same with original token"
				, anotherToken.GetMasterKeyId(), masterKeyId);
			NUnit.Framework.Assert.AreEqual("RMIdentifier from proto is not the same with original token"
				, anotherToken.GetRMIdentifier(), rmIdentifier);
			NUnit.Framework.Assert.AreEqual("Priority from proto is not the same with original token"
				, anotherToken.GetPriority(), priority);
			NUnit.Framework.Assert.AreEqual("CreationTime from proto is not the same with original token"
				, anotherToken.GetCreationTime(), creationTime);
			NUnit.Framework.Assert.IsNull(anotherToken.GetLogAggregationContext());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRMDelegationTokenIdentifier()
		{
			Text owner = new Text("user1");
			Text renewer = new Text("user2");
			Text realUser = new Text("user3");
			long issueDate = 1;
			long maxDate = 2;
			int sequenceNumber = 3;
			int masterKeyId = 4;
			RMDelegationTokenIdentifier token = new RMDelegationTokenIdentifier(owner, renewer
				, realUser);
			token.SetIssueDate(issueDate);
			token.SetMaxDate(maxDate);
			token.SetSequenceNumber(sequenceNumber);
			token.SetMasterKeyId(masterKeyId);
			RMDelegationTokenIdentifier anotherToken = new RMDelegationTokenIdentifier();
			byte[] tokenContent = token.GetBytes();
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(tokenContent, tokenContent.Length);
			anotherToken.ReadFields(dib);
			dib.Close();
			// verify the whole record equals with original record
			NUnit.Framework.Assert.AreEqual("Token is not the same after serialization " + "and deserialization."
				, token, anotherToken);
			NUnit.Framework.Assert.AreEqual("owner from proto is not the same with original token"
				, anotherToken.GetOwner(), owner);
			NUnit.Framework.Assert.AreEqual("renewer from proto is not the same with original token"
				, anotherToken.GetRenewer(), renewer);
			NUnit.Framework.Assert.AreEqual("realUser from proto is not the same with original token"
				, anotherToken.GetRealUser(), realUser);
			NUnit.Framework.Assert.AreEqual("issueDate from proto is not the same with original token"
				, anotherToken.GetIssueDate(), issueDate);
			NUnit.Framework.Assert.AreEqual("maxDate from proto is not the same with original token"
				, anotherToken.GetMaxDate(), maxDate);
			NUnit.Framework.Assert.AreEqual("sequenceNumber from proto is not the same with original token"
				, anotherToken.GetSequenceNumber(), sequenceNumber);
			NUnit.Framework.Assert.AreEqual("masterKeyId from proto is not the same with original token"
				, anotherToken.GetMasterKeyId(), masterKeyId);
			// Test getProto    
			RMDelegationTokenIdentifier token1 = new RMDelegationTokenIdentifier(owner, renewer
				, realUser);
			token1.SetIssueDate(issueDate);
			token1.SetMaxDate(maxDate);
			token1.SetSequenceNumber(sequenceNumber);
			token1.SetMasterKeyId(masterKeyId);
			YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto tokenProto = token1.GetProto
				();
			// Write token proto to stream
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream @out = new DataOutputStream(baos);
			tokenProto.WriteTo(@out);
			// Read token
			byte[] tokenData = baos.ToByteArray();
			RMDelegationTokenIdentifier readToken = new RMDelegationTokenIdentifier();
			DataInputBuffer db = new DataInputBuffer();
			db.Reset(tokenData, tokenData.Length);
			readToken.ReadFields(db);
			// Verify if read token equals with original token
			NUnit.Framework.Assert.AreEqual("Token from getProto is not the same after " + "serialization and deserialization."
				, token1, readToken);
			db.Close();
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTimelineDelegationTokenIdentifier()
		{
			Text owner = new Text("user1");
			Text renewer = new Text("user2");
			Text realUser = new Text("user3");
			long issueDate = 1;
			long maxDate = 2;
			int sequenceNumber = 3;
			int masterKeyId = 4;
			TimelineDelegationTokenIdentifier token = new TimelineDelegationTokenIdentifier(owner
				, renewer, realUser);
			token.SetIssueDate(issueDate);
			token.SetMaxDate(maxDate);
			token.SetSequenceNumber(sequenceNumber);
			token.SetMasterKeyId(masterKeyId);
			TimelineDelegationTokenIdentifier anotherToken = new TimelineDelegationTokenIdentifier
				();
			byte[] tokenContent = token.GetBytes();
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(tokenContent, tokenContent.Length);
			anotherToken.ReadFields(dib);
			// verify the whole record equals with original record
			NUnit.Framework.Assert.AreEqual("Token is not the same after serialization " + "and deserialization."
				, token, anotherToken);
			NUnit.Framework.Assert.AreEqual("owner from proto is not the same with original token"
				, anotherToken.GetOwner(), owner);
			NUnit.Framework.Assert.AreEqual("renewer from proto is not the same with original token"
				, anotherToken.GetRenewer(), renewer);
			NUnit.Framework.Assert.AreEqual("realUser from proto is not the same with original token"
				, anotherToken.GetRealUser(), realUser);
			NUnit.Framework.Assert.AreEqual("issueDate from proto is not the same with original token"
				, anotherToken.GetIssueDate(), issueDate);
			NUnit.Framework.Assert.AreEqual("maxDate from proto is not the same with original token"
				, anotherToken.GetMaxDate(), maxDate);
			NUnit.Framework.Assert.AreEqual("sequenceNumber from proto is not the same with original token"
				, anotherToken.GetSequenceNumber(), sequenceNumber);
			NUnit.Framework.Assert.AreEqual("masterKeyId from proto is not the same with original token"
				, anotherToken.GetMasterKeyId(), masterKeyId);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestParseTimelineDelegationTokenIdentifierRenewer()
		{
			// Server side when generation a timeline DT
			Configuration conf = new YarnConfiguration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthToLocal, "RULE:[2:$1@$0]([nr]m@.*EXAMPLE.COM)s/.*/yarn/"
				);
			HadoopKerberosName.SetConfiguration(conf);
			Text owner = new Text("owner");
			Text renewer = new Text("rm/localhost@EXAMPLE.COM");
			Text realUser = new Text("realUser");
			TimelineDelegationTokenIdentifier token = new TimelineDelegationTokenIdentifier(owner
				, renewer, realUser);
			NUnit.Framework.Assert.AreEqual(new Text("yarn"), token.GetRenewer());
		}
	}
}
