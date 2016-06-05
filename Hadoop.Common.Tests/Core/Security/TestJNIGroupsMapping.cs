using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security
{
	public class TestJNIGroupsMapping
	{
		[SetUp]
		public virtual void IsNativeCodeLoaded()
		{
			Assume.AssumeTrue(NativeCodeLoader.IsNativeCodeLoaded());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestJNIGroupsMapping()
		{
			//for the user running the test, check whether the 
			//ShellBasedUnixGroupsMapping and the JniBasedUnixGroupsMapping
			//return the same groups
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			TestForUser(user);
			//check for a dummy non-existent user (both the implementations should
			//return an empty list
			TestForUser("fooBarBaz1234DoesNotExist");
		}

		/// <exception cref="System.Exception"/>
		private void TestForUser(string user)
		{
			GroupMappingServiceProvider g = new ShellBasedUnixGroupsMapping();
			IList<string> shellBasedGroups = g.GetGroups(user);
			g = new JniBasedUnixGroupsMapping();
			IList<string> jniBasedGroups = g.GetGroups(user);
			string[] shellBasedGroupsArray = Collections.ToArray(shellBasedGroups, new 
				string[0]);
			Arrays.Sort(shellBasedGroupsArray);
			string[] jniBasedGroupsArray = Collections.ToArray(jniBasedGroups, new string
				[0]);
			Arrays.Sort(jniBasedGroupsArray);
			if (!Arrays.Equals(shellBasedGroupsArray, jniBasedGroupsArray))
			{
				NUnit.Framework.Assert.Fail("Groups returned by " + typeof(ShellBasedUnixGroupsMapping
					).GetCanonicalName() + " and " + typeof(JniBasedUnixGroupsMapping).GetCanonicalName
					() + " didn't match for " + user);
			}
		}
	}
}
