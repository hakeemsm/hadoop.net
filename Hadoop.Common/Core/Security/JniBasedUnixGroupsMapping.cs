using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// A JNI-based implementation of
	/// <see cref="GroupMappingServiceProvider"/>
	/// 
	/// that invokes libC calls to get the group
	/// memberships of a given user.
	/// </summary>
	public class JniBasedUnixGroupsMapping : GroupMappingServiceProvider
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(JniBasedUnixGroupsMapping
			));

		static JniBasedUnixGroupsMapping()
		{
			if (!NativeCodeLoader.IsNativeCodeLoaded())
			{
				throw new RuntimeException("Bailing out since native library couldn't " + "be loaded"
					);
			}
			AnchorNative();
			Log.Debug("Using JniBasedUnixGroupsMapping for Group resolution");
		}

		/// <summary>Set up our JNI resources.</summary>
		/// <exception cref="RuntimeException">if setup fails.</exception>
		internal static void AnchorNative()
		{
		}

		/// <summary>Get the set of groups associated with a user.</summary>
		/// <param name="username">The user name</param>
		/// <returns>The set of groups associated with a user.</returns>
		internal static string[] GetGroupsForUser(string username)
		{
		}

		/// <summary>Log an error message about a group.</summary>
		/// <remarks>Log an error message about a group.  Used from JNI.</remarks>
		private static void LogError(int groupId, string error)
		{
			Log.Error("error looking up the name of group " + groupId + ": " + error);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetGroups(string user)
		{
			string[] groups = new string[0];
			try
			{
				groups = GetGroupsForUser(user);
			}
			catch (Exception e)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Error getting groups for " + user, e);
				}
				else
				{
					Log.Info("Error getting groups for " + user + ": " + e.Message);
				}
			}
			return Arrays.AsList(groups);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsRefresh()
		{
		}

		// does nothing in this provider of user to groups mapping
		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsAdd(IList<string> groups)
		{
		}
		// does nothing in this provider of user to groups mapping
	}
}
