using Sharpen;

namespace org.apache.hadoop.security.authorize
{
	/// <summary>Class representing a configured access control list.</summary>
	public class AccessControlList : org.apache.hadoop.io.Writable
	{
		static AccessControlList()
		{
			// register a ctor
			org.apache.hadoop.io.WritableFactories.setFactory(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.security.authorize.AccessControlList)), new _WritableFactory_49
				());
		}

		private sealed class _WritableFactory_49 : org.apache.hadoop.io.WritableFactory
		{
			public _WritableFactory_49()
			{
			}

			public org.apache.hadoop.io.Writable newInstance()
			{
				return new org.apache.hadoop.security.authorize.AccessControlList();
			}
		}

		public const string WILDCARD_ACL_VALUE = "*";

		private const int INITIAL_CAPACITY = 256;

		private System.Collections.Generic.ICollection<string> users;

		private System.Collections.Generic.ICollection<string> groups;

		private bool allAllowed;

		private org.apache.hadoop.security.Groups groupsMapping = org.apache.hadoop.security.Groups
			.getUserToGroupsMappingService(new org.apache.hadoop.conf.Configuration());

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
			buildACL(aclString.split(" ", 2));
		}

		/// <summary>
		/// Construct a new ACL from String representation of users and groups
		/// The arguments are comma separated lists
		/// </summary>
		/// <param name="users">comma separated list of users</param>
		/// <param name="groups">comma separated list of groups</param>
		public AccessControlList(string users, string groups)
		{
			buildACL(new string[] { users, groups });
		}

		/// <summary>Build ACL from the given two Strings.</summary>
		/// <remarks>
		/// Build ACL from the given two Strings.
		/// The Strings contain comma separated values.
		/// </remarks>
		/// <param name="aclString">build ACL from array of Strings</param>
		private void buildACL(string[] userGroupStrings)
		{
			users = new java.util.HashSet<string>();
			groups = new java.util.HashSet<string>();
			foreach (string aclPart in userGroupStrings)
			{
				if (aclPart != null && isWildCardACLValue(aclPart))
				{
					allAllowed = true;
					break;
				}
			}
			if (!allAllowed)
			{
				if (userGroupStrings.Length >= 1 && userGroupStrings[0] != null)
				{
					users = org.apache.hadoop.util.StringUtils.getTrimmedStringCollection(userGroupStrings
						[0]);
				}
				if (userGroupStrings.Length == 2 && userGroupStrings[1] != null)
				{
					groups = org.apache.hadoop.util.StringUtils.getTrimmedStringCollection(userGroupStrings
						[1]);
					groupsMapping.cacheGroupsAdd(new System.Collections.Generic.LinkedList<string>(groups
						));
				}
			}
		}

		/// <summary>Checks whether ACL string contains wildcard</summary>
		/// <param name="aclString">check this ACL string for wildcard</param>
		/// <returns>true if ACL string contains wildcard false otherwise</returns>
		private bool isWildCardACLValue(string aclString)
		{
			if (aclString.contains(WILDCARD_ACL_VALUE) && aclString.Trim().Equals(WILDCARD_ACL_VALUE
				))
			{
				return true;
			}
			return false;
		}

		public virtual bool isAllAllowed()
		{
			return allAllowed;
		}

		/// <summary>Add user to the names of users allowed for this service.</summary>
		/// <param name="user">The user name</param>
		public virtual void addUser(string user)
		{
			if (isWildCardACLValue(user))
			{
				throw new System.ArgumentException("User " + user + " can not be added");
			}
			if (!isAllAllowed())
			{
				users.add(user);
			}
		}

		/// <summary>Add group to the names of groups allowed for this service.</summary>
		/// <param name="group">The group name</param>
		public virtual void addGroup(string group)
		{
			if (isWildCardACLValue(group))
			{
				throw new System.ArgumentException("Group " + group + " can not be added");
			}
			if (!isAllAllowed())
			{
				System.Collections.Generic.IList<string> groupsList = new System.Collections.Generic.LinkedList
					<string>();
				groupsList.add(group);
				groupsMapping.cacheGroupsAdd(groupsList);
				groups.add(group);
			}
		}

		/// <summary>Remove user from the names of users allowed for this service.</summary>
		/// <param name="user">The user name</param>
		public virtual void removeUser(string user)
		{
			if (isWildCardACLValue(user))
			{
				throw new System.ArgumentException("User " + user + " can not be removed");
			}
			if (!isAllAllowed())
			{
				users.remove(user);
			}
		}

		/// <summary>Remove group from the names of groups allowed for this service.</summary>
		/// <param name="group">The group name</param>
		public virtual void removeGroup(string group)
		{
			if (isWildCardACLValue(group))
			{
				throw new System.ArgumentException("Group " + group + " can not be removed");
			}
			if (!isAllAllowed())
			{
				groups.remove(group);
			}
		}

		/// <summary>Get the names of users allowed for this service.</summary>
		/// <returns>the set of user names. the set must not be modified.</returns>
		public virtual System.Collections.Generic.ICollection<string> getUsers()
		{
			return users;
		}

		/// <summary>Get the names of user groups allowed for this service.</summary>
		/// <returns>the set of group names. the set must not be modified.</returns>
		public virtual System.Collections.Generic.ICollection<string> getGroups()
		{
			return groups;
		}

		/// <summary>
		/// Checks if a user represented by the provided
		/// <see cref="org.apache.hadoop.security.UserGroupInformation"/>
		/// is a member of the Access Control List
		/// </summary>
		/// <param name="ugi">UserGroupInformation to check if contained in the ACL</param>
		/// <returns>true if ugi is member of the list</returns>
		public bool isUserInList(org.apache.hadoop.security.UserGroupInformation ugi)
		{
			if (allAllowed || users.contains(ugi.getShortUserName()))
			{
				return true;
			}
			else
			{
				if (!groups.isEmpty())
				{
					foreach (string group in ugi.getGroupNames())
					{
						if (groups.contains(group))
						{
							return true;
						}
					}
				}
			}
			return false;
		}

		public virtual bool isUserAllowed(org.apache.hadoop.security.UserGroupInformation
			 ugi)
		{
			return isUserInList(ugi);
		}

		/// <summary>Returns descriptive way of users and groups that are part of this ACL.</summary>
		/// <remarks>
		/// Returns descriptive way of users and groups that are part of this ACL.
		/// Use
		/// <see cref="getAclString()"/>
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
				if (users.isEmpty() && groups.isEmpty())
				{
					str = "No users are allowed";
				}
				else
				{
					string usersStr = null;
					string groupsStr = null;
					if (!users.isEmpty())
					{
						usersStr = users.ToString();
					}
					if (!groups.isEmpty())
					{
						groupsStr = groups.ToString();
					}
					if (!users.isEmpty() && !groups.isEmpty())
					{
						str = "Users " + usersStr + " and members of the groups " + groupsStr + " are allowed";
					}
					else
					{
						if (!users.isEmpty())
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
		public virtual string getAclString()
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder(INITIAL_CAPACITY);
			if (allAllowed)
			{
				sb.Append('*');
			}
			else
			{
				sb.Append(getUsersString());
				sb.Append(" ");
				sb.Append(getGroupsString());
			}
			return sb.ToString();
		}

		/// <summary>Serializes the AccessControlList object</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			string aclString = getAclString();
			org.apache.hadoop.io.Text.writeString(@out, aclString);
		}

		/// <summary>Deserializes the AccessControlList object</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			string aclString = org.apache.hadoop.io.Text.readString(@in);
			buildACL(aclString.split(" ", 2));
		}

		/// <summary>Returns comma-separated concatenated single String of the set 'users'</summary>
		/// <returns>comma separated list of users</returns>
		private string getUsersString()
		{
			return getString(users);
		}

		/// <summary>Returns comma-separated concatenated single String of the set 'groups'</summary>
		/// <returns>comma separated list of groups</returns>
		private string getGroupsString()
		{
			return getString(groups);
		}

		/// <summary>
		/// Returns comma-separated concatenated single String of all strings of
		/// the given set
		/// </summary>
		/// <param name="strings">set of strings to concatenate</param>
		private string getString(System.Collections.Generic.ICollection<string> strings)
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder(INITIAL_CAPACITY);
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
