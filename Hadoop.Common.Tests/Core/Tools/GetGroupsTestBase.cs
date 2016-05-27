using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Tools
{
	public abstract class GetGroupsTestBase
	{
		protected internal Configuration conf;

		private UserGroupInformation testUser1;

		private UserGroupInformation testUser2;

		protected internal abstract Tool GetTool(TextWriter o);

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetUpUsers()
		{
			// Make sure the current user's info is in the list of test users.
			UserGroupInformation currentUser = UserGroupInformation.GetCurrentUser();
			UserGroupInformation.CreateUserForTesting(currentUser.GetUserName(), currentUser.
				GetGroupNames());
			testUser1 = UserGroupInformation.CreateUserForTesting("foo", new string[] { "bar"
				, "baz" });
			testUser2 = UserGroupInformation.CreateUserForTesting("fiz", new string[] { "buz"
				, "boz" });
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoUserGiven()
		{
			string actualOutput = RunTool(conf, new string[0], true);
			UserGroupInformation currentUser = UserGroupInformation.GetCurrentUser();
			NUnit.Framework.Assert.AreEqual("No user provided should default to current user"
				, GetExpectedOutput(currentUser), actualOutput);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExistingUser()
		{
			string actualOutput = RunTool(conf, new string[] { testUser1.GetUserName() }, true
				);
			NUnit.Framework.Assert.AreEqual("Show only the output of the user given", GetExpectedOutput
				(testUser1), actualOutput);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleExistingUsers()
		{
			string actualOutput = RunTool(conf, new string[] { testUser1.GetUserName(), testUser2
				.GetUserName() }, true);
			NUnit.Framework.Assert.AreEqual("Show the output for both users given", GetExpectedOutput
				(testUser1) + GetExpectedOutput(testUser2), actualOutput);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonExistentUser()
		{
			string actualOutput = RunTool(conf, new string[] { "does-not-exist" }, true);
			NUnit.Framework.Assert.AreEqual("Show the output for only the user given, with no groups"
				, GetExpectedOutput(UserGroupInformation.CreateRemoteUser("does-not-exist")), actualOutput
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleNonExistingUsers()
		{
			string actualOutput = RunTool(conf, new string[] { "does-not-exist1", "does-not-exist2"
				 }, true);
			NUnit.Framework.Assert.AreEqual("Show the output for only the user given, with no groups"
				, GetExpectedOutput(UserGroupInformation.CreateRemoteUser("does-not-exist1")) + 
				GetExpectedOutput(UserGroupInformation.CreateRemoteUser("does-not-exist2")), actualOutput
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExistingInterleavedWithNonExistentUsers()
		{
			string actualOutput = RunTool(conf, new string[] { "does-not-exist1", testUser1.GetUserName
				(), "does-not-exist2", testUser2.GetUserName() }, true);
			NUnit.Framework.Assert.AreEqual("Show the output for only the user given, with no groups"
				, GetExpectedOutput(UserGroupInformation.CreateRemoteUser("does-not-exist1")) + 
				GetExpectedOutput(testUser1) + GetExpectedOutput(UserGroupInformation.CreateRemoteUser
				("does-not-exist2")) + GetExpectedOutput(testUser2), actualOutput);
		}

		private static string GetExpectedOutput(UserGroupInformation user)
		{
			string expectedOutput = user.GetUserName() + " :";
			foreach (string group in user.GetGroupNames())
			{
				expectedOutput += " " + group;
			}
			return expectedOutput + Runtime.GetProperty("line.separator");
		}

		/// <exception cref="System.Exception"/>
		private string RunTool(Configuration conf, string[] args, bool success)
		{
			ByteArrayOutputStream o = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(o, true);
			try
			{
				int ret = ToolRunner.Run(GetTool(@out), args);
				NUnit.Framework.Assert.AreEqual(success, ret == 0);
				return o.ToString();
			}
			finally
			{
				o.Close();
				@out.Close();
			}
		}
	}
}
