using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security.Authorize
{
	/// <summary>Class representing a configured access control list.</summary>
	public class AccessControlList : IWritable
	{
		static AccessControlList()
		{
			// register a ctor
			WritableFactories.SetFactory(typeof(Org.Apache.Hadoop.Security.Authorize.AccessControlList
				), new _WritableFactory_49());
		}

		private sealed class _WritableFactory_49 : WritableFactory
		{
			public _WritableFactory_49()
			{
			}

			public IWritable NewInstance()
			{
				return new Org.Apache.Hadoop.Security.Authorize.AccessControlList();
			}
		}

		public const string WildcardAclValue = "*";

		private const int InitialCapacity = 256;

		private ICollection<string> users;

		private ICollection<string> groups;

		private bool allAllowed;

		private Groups groupsMapping = Groups.GetUserToGroupsMappingService(new Configuration
			());

		/// <summary>This constructor exists primarily for AccessControlList to be Writable.</summary>
		public AccessControlList()
		{
		}

		/// <summary>Construct a new ACL from a String representation of the same.</summary>
		/// <remarks>
		/// Construct a new ACL from a String representation of the same.
		/// The String is a a comma separated list of users and groups.
		/// The user list comes first and is separated by a space followed
		/// by the group list. For e.g. "user1,user2 group1,group2"
		/// </remarks>
		/// <param name="aclString">String representation of the ACL</param>
		public AccessControlList(string aclString)
		{
			// Indicates an ACL string that represents access to all users
			// Set of users who are granted access.
			// Set of groups which are granted access
			// Whether all users are granted access.
			BuildACL(aclString.Split(" ", 2));
		}

		/// <summary>
		/// Construct a new ACL from String representation of users and groups
		/// The arguments are comma separated lists
		/// </summary>
		/// <param name="users">comma separated list of users</param>
		/// <param name="groups">comma separated list of groups</param>
		public AccessControlList(string users, string groups)
		{
			BuildACL(new string[] { users, groups });
		}

		/// <summary>Build ACL from the given two Strings.</summary>
		/// <remarks>
		/// Build ACL from the given two Strings.
		/// The Strings contain comma separated values.
		/// </remarks>
		/// <param name="aclString">build ACL from array of Strings</param>
		private void BuildACL(string[] userGroupStrings)
		{
			users = new HashSet<string>();
			groups = new HashSet<string>();
			foreach (string aclPart in userGroupStrings)
			{
				if (aclPart != null && IsWildCardACLValue(aclPart))
				{
					allAllowed = true;
					break;
				}
			}
			if (!allAllowed)
			{
				if (userGroupStrings.Length >= 1 && userGroupStrings[0] != null)
				{
					users = StringUtils.GetTrimmedStringCollection(userGroupStrings[0]);
				}
				if (userGroupStrings.Length == 2 && userGroupStrings[1] != null)
				{
					groups = StringUtils.GetTrimmedStringCollection(userGroupStrings[1]);
					groupsMapping.CacheGroupsAdd(new List<string>(groups));
				}
			}
		}

		/// <summary>Checks whether ACL string contains wildcard</summary>
		/// <param name="aclString">check this ACL string for wildcard</param>
		/// <returns>true if ACL string contains wildcard false otherwise</returns>
		private bool IsWildCardACLValue(string aclString)
		{
			if (aclString.Contains(WildcardAclValue) && aclString.Trim().Equals(WildcardAclValue
				))
			{
				return true;
			}
			return false;
		}

		public virtual bool IsAllAllowed()
		{
			return allAllowed;
		}

		/// <summary>Add user to the names of users allowed for this service.</summary>
		/// <param name="user">The user name</param>
		public virtual void AddUser(string user)
		{
			if (IsWildCardACLValue(user))
			{
				throw new ArgumentException("User " + user + " can not be added");
			}
			if (!IsAllAllowed())
			{
				users.AddItem(user);
			}
		}

		/// <summary>Add group to the names of groups allowed for this service.</summary>
		/// <param name="group">The group name</param>
		public virtual void AddGroup(string group)
		{
			if (IsWildCardACLValue(group))
			{
				throw new ArgumentException("Group " + group + " can not be added");
			}
			if (!IsAllAllowed())
			{
				IList<string> groupsList = new List<string>();
				groupsList.AddItem(group);
				groupsMapping.CacheGroupsAdd(groupsList);
				groups.AddItem(group);
			}
		}

		/// <summary>Remove user from the names of users allowed for this service.</summary>
		/// <param name="user">The user name</param>
		public virtual void RemoveUser(string user)
		{
			if (IsWildCardACLValue(user))
			{
				throw new ArgumentException("User " + user + " can not be removed");
			}
			if (!IsAllAllowed())
			{
				users.Remove(user);
			}
		}

		/// <summary>Remove group from the names of groups allowed for this service.</summary>
		/// <param name="group">The group name</param>
		public virtual void RemoveGroup(string group)
		{
			if (IsWildCardACLValue(group))
			{
				throw new ArgumentException("Group " + group + " can not be removed");
			}
			if (!IsAllAllowed())
			{
				groups.Remove(group);
			}
		}

		/// <summary>Get the names of users allowed for this service.</summary>
		/// <returns>the set of user names. the set must not be modified.</returns>
		public virtual ICollection<string> GetUsers()
		{
			return users;
		}

		/// <summary>Get the names of user groups allowed for this service.</summary>
		/// <returns>the set of group names. the set must not be modified.</returns>
		public virtual ICollection<string> GetGroups()
		{
			return groups;
		}

		/// <summary>
		/// Checks if a user represented by the provided
		/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation"/>
		/// is a member of the Access Control List
		/// </summary>
		/// <param name="ugi">UserGroupInformation to check if contained in the ACL</param>
		/// <returns>true if ugi is member of the list</returns>
		public bool IsUserInList(UserGroupInformation ugi)
		{
			if (allAllowed || users.Contains(ugi.GetShortUserName()))
			{
				return true;
			}
			else
			{
				if (!groups.IsEmpty())
				{
					foreach (string group in ugi.GetGroupNames())
					{
						if (groups.Contains(group))
						{
							return true;
						}
					}
				}
			}
			return false;
		}

		public virtual bool IsUserAllowed(UserGroupInformation ugi)
		{
			return IsUserInList(ugi);
		}

		/// <summary>Returns descriptive way of users and groups that are part of this ACL.</summary>
		/// <remarks>
		/// Returns descriptive way of users and groups that are part of this ACL.
		/// Use
		/// <see cref="GetAclString()"/>
		/// to get the exact String that can be given to
		/// the constructor of AccessControlList to create a new instance.
		/// </remarks>
		public override string ToString()
		{
			string str = null;
			if (allAllowed)
			{
				str = "All users are allowed";
			}
			else
			{
				if (users.IsEmpty() && groups.IsEmpty())
				{
					str = "No users are allowed";
				}
				else
				{
					string usersStr = null;
					string groupsStr = null;
					if (!users.IsEmpty())
					{
						usersStr = users.ToString();
					}
					if (!groups.IsEmpty())
					{
						groupsStr = groups.ToString();
					}
					if (!users.IsEmpty() && !groups.IsEmpty())
					{
						str = "Users " + usersStr + " and members of the groups " + groupsStr + " are allowed";
					}
					else
					{
						if (!users.IsEmpty())
						{
							str = "Users " + usersStr + " are allowed";
						}
						else
						{
							// users is empty array and groups is nonempty
							str = "Members of the groups " + groupsStr + " are allowed";
						}
					}
				}
			}
			return str;
		}

		/// <summary>
		/// Returns the access control list as a String that can be used for building a
		/// new instance by sending it to the constructor of
		/// <see cref="AccessControlList"/>
		/// .
		/// </summary>
		public virtual string GetAclString()
		{
			StringBuilder sb = new StringBuilder(InitialCapacity);
			if (allAllowed)
			{
				sb.Append('*');
			}
			else
			{
				sb.Append(GetUsersString());
				sb.Append(" ");
				sb.Append(GetGroupsString());
			}
			return sb.ToString();
		}

		/// <summary>Serializes the AccessControlList object</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			string aclString = GetAclString();
			Text.WriteString(@out, aclString);
		}

		/// <summary>Deserializes the AccessControlList object</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			string aclString = Text.ReadString(@in);
			BuildACL(aclString.Split(" ", 2));
		}

		/// <summary>Returns comma-separated concatenated single String of the set 'users'</summary>
		/// <returns>comma separated list of users</returns>
		private string GetUsersString()
		{
			return GetString(users);
		}

		/// <summary>Returns comma-separated concatenated single String of the set 'groups'</summary>
		/// <returns>comma separated list of groups</returns>
		private string GetGroupsString()
		{
			return GetString(groups);
		}

		/// <summary>
		/// Returns comma-separated concatenated single String of all strings of
		/// the given set
		/// </summary>
		/// <param name="strings">set of strings to concatenate</param>
		private string GetString(ICollection<string> strings)
		{
			StringBuilder sb = new StringBuilder(InitialCapacity);
			bool first = true;
			foreach (string str in strings)
			{
				if (!first)
				{
					sb.Append(",");
				}
				else
				{
					first = false;
				}
				sb.Append(str);
			}
			return sb.ToString();
		}
	}
}
