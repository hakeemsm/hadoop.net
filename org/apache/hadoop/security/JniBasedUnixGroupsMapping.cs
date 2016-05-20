using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>
	/// A JNI-based implementation of
	/// <see cref="GroupMappingServiceProvider"/>
	/// 
	/// that invokes libC calls to get the group
	/// memberships of a given user.
	/// </summary>
	public class JniBasedUnixGroupsMapping : org.apache.hadoop.security.GroupMappingServiceProvider
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.JniBasedUnixGroupsMapping
			)));

		static JniBasedUnixGroupsMapping()
		{
			if (!org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
			{
				throw new System.Exception("Bailing out since native library couldn't " + "be loaded"
					);
			}
			anchorNative();
			LOG.debug("Using JniBasedUnixGroupsMapping for Group resolution");
		}

		/// <summary>Set up our JNI resources.</summary>
		/// <exception cref="System.Exception">if setup fails.</exception>
		internal static void anchorNative()
		{
		}

		/// <summary>Get the set of groups associated with a user.</summary>
		/// <param name="username">The user name</param>
		/// <returns>The set of groups associated with a user.</returns>
		internal static string[] getGroupsForUser(string username)
		{
		}

		/// <summary>Log an error message about a group.</summary>
		/// <remarks>Log an error message about a group.  Used from JNI.</remarks>
		private static void logError(int groupId, string error)
		{
			LOG.error("error looking up the name of group " + groupId + ": " + error);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> getGroups(string user)
		{
			string[] groups = new string[0];
			try
			{
				groups = getGroupsForUser(user);
			}
			catch (System.Exception e)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Error getting groups for " + user, e);
				}
				else
				{
					LOG.info("Error getting groups for " + user + ": " + e.Message);
				}
			}
			return java.util.Arrays.asList(groups);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void cacheGroupsRefresh()
		{
		}

		// does nothing in this provider of user to groups mapping
		/// <exception cref="System.IO.IOException"/>
		public override void cacheGroupsAdd(System.Collections.Generic.IList<string> groups
			)
		{
		}
		// does nothing in this provider of user to groups mapping
	}
}
