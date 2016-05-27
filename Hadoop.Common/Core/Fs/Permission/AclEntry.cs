using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Permission
{
	/// <summary>Defines a single entry in an ACL.</summary>
	/// <remarks>
	/// Defines a single entry in an ACL.  An ACL entry has a type (user, group,
	/// mask, or other), an optional name (referring to a specific user or group), a
	/// set of permissions (any combination of read, write and execute), and a scope
	/// (access or default).  AclEntry instances are immutable.  Use a
	/// <see cref="Builder"/>
	/// to create a new instance.
	/// </remarks>
	public class AclEntry
	{
		private readonly AclEntryType type;

		private readonly string name;

		private readonly FsAction permission;

		private readonly AclEntryScope scope;

		/// <summary>Returns the ACL entry type.</summary>
		/// <returns>AclEntryType ACL entry type</returns>
		public virtual AclEntryType GetType()
		{
			return type;
		}

		/// <summary>Returns the optional ACL entry name.</summary>
		/// <returns>String ACL entry name, or null if undefined</returns>
		public virtual string GetName()
		{
			return name;
		}

		/// <summary>Returns the set of permissions in the ACL entry.</summary>
		/// <returns>FsAction set of permissions in the ACL entry</returns>
		public virtual FsAction GetPermission()
		{
			return permission;
		}

		/// <summary>Returns the scope of the ACL entry.</summary>
		/// <returns>AclEntryScope scope of the ACL entry</returns>
		public virtual AclEntryScope GetScope()
		{
			return scope;
		}

		public override bool Equals(object o)
		{
			if (o == null)
			{
				return false;
			}
			if (GetType() != o.GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.FS.Permission.AclEntry other = (Org.Apache.Hadoop.FS.Permission.AclEntry
				)o;
			return Objects.Equal(type, other.type) && Objects.Equal(name, other.name) && Objects
				.Equal(permission, other.permission) && Objects.Equal(scope, other.scope);
		}

		public override int GetHashCode()
		{
			return Objects.HashCode(type, name, permission, scope);
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			if (scope == AclEntryScope.Default)
			{
				sb.Append("default:");
			}
			if (type != null)
			{
				sb.Append(StringUtils.ToLowerCase(type.ToString()));
			}
			sb.Append(':');
			if (name != null)
			{
				sb.Append(name);
			}
			sb.Append(':');
			if (permission != null)
			{
				sb.Append(permission.Symbol);
			}
			return sb.ToString();
		}

		/// <summary>Builder for creating new AclEntry instances.</summary>
		public class Builder
		{
			private AclEntryType type;

			private string name;

			private FsAction permission;

			private AclEntryScope scope = AclEntryScope.Access;

			/// <summary>Sets the ACL entry type.</summary>
			/// <param name="type">AclEntryType ACL entry type</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual AclEntry.Builder SetType(AclEntryType type)
			{
				this.type = type;
				return this;
			}

			/// <summary>Sets the optional ACL entry name.</summary>
			/// <param name="name">String optional ACL entry name</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual AclEntry.Builder SetName(string name)
			{
				if (name != null && !name.IsEmpty())
				{
					this.name = name;
				}
				return this;
			}

			/// <summary>Sets the set of permissions in the ACL entry.</summary>
			/// <param name="permission">FsAction set of permissions in the ACL entry</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual AclEntry.Builder SetPermission(FsAction permission)
			{
				this.permission = permission;
				return this;
			}

			/// <summary>Sets the scope of the ACL entry.</summary>
			/// <remarks>
			/// Sets the scope of the ACL entry.  If this method is not called, then the
			/// builder assumes
			/// <see cref="AclEntryScope.Access"/>
			/// .
			/// </remarks>
			/// <param name="scope">AclEntryScope scope of the ACL entry</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual AclEntry.Builder SetScope(AclEntryScope scope)
			{
				this.scope = scope;
				return this;
			}

			/// <summary>Builds a new AclEntry populated with the set properties.</summary>
			/// <returns>AclEntry new AclEntry</returns>
			public virtual AclEntry Build()
			{
				return new AclEntry(type, name, permission, scope);
			}
		}

		/// <summary>Private constructor.</summary>
		/// <param name="type">AclEntryType ACL entry type</param>
		/// <param name="name">String optional ACL entry name</param>
		/// <param name="permission">FsAction set of permissions in the ACL entry</param>
		/// <param name="scope">AclEntryScope scope of the ACL entry</param>
		private AclEntry(AclEntryType type, string name, FsAction permission, AclEntryScope
			 scope)
		{
			this.type = type;
			this.name = name;
			this.permission = permission;
			this.scope = scope;
		}

		/// <summary>
		/// Parses a string representation of an ACL spec into a list of AclEntry
		/// objects.
		/// </summary>
		/// <remarks>
		/// Parses a string representation of an ACL spec into a list of AclEntry
		/// objects. Example: "user::rwx,user:foo:rw-,group::r--,other::---"
		/// </remarks>
		/// <param name="aclSpec">String representation of an ACL spec.</param>
		/// <param name="includePermission">
		/// for setAcl operations this will be true. i.e. AclSpec should
		/// include permissions.<br />
		/// But for removeAcl operation it will be false. i.e. AclSpec should
		/// not contain permissions.<br />
		/// Example: "user:foo,group:bar"
		/// </param>
		/// <returns>
		/// Returns list of
		/// <see cref="AclEntry"/>
		/// parsed
		/// </returns>
		public static IList<AclEntry> ParseAclSpec(string aclSpec, bool includePermission
			)
		{
			IList<AclEntry> aclEntries = new AList<AclEntry>();
			ICollection<string> aclStrings = StringUtils.GetStringCollection(aclSpec, ",");
			foreach (string aclStr in aclStrings)
			{
				AclEntry aclEntry = ParseAclEntry(aclStr, includePermission);
				aclEntries.AddItem(aclEntry);
			}
			return aclEntries;
		}

		/// <summary>Parses a string representation of an ACL into a AclEntry object.<br /></summary>
		/// <param name="aclStr">
		/// String representation of an ACL.<br />
		/// Example: "user:foo:rw-"
		/// </param>
		/// <param name="includePermission">
		/// for setAcl operations this will be true. i.e. Acl should include
		/// permissions.<br />
		/// But for removeAcl operation it will be false. i.e. Acl should not
		/// contain permissions.<br />
		/// Example: "user:foo,group:bar,mask::"
		/// </param>
		/// <returns>
		/// Returns an
		/// <see cref="AclEntry"/>
		/// object
		/// </returns>
		public static AclEntry ParseAclEntry(string aclStr, bool includePermission)
		{
			AclEntry.Builder builder = new AclEntry.Builder();
			// Here "::" represent one empty string.
			// StringUtils.getStringCollection() will ignore this.
			string[] split = aclStr.Split(":");
			if (split.Length == 0)
			{
				throw new HadoopIllegalArgumentException("Invalid <aclSpec> : " + aclStr);
			}
			int index = 0;
			if ("default".Equals(split[0]))
			{
				// default entry
				index++;
				builder.SetScope(AclEntryScope.Default);
			}
			if (split.Length <= index)
			{
				throw new HadoopIllegalArgumentException("Invalid <aclSpec> : " + aclStr);
			}
			AclEntryType aclType = null;
			try
			{
				aclType = Enum.ValueOf<AclEntryType>(StringUtils.ToUpperCase(split[index]));
				builder.SetType(aclType);
				index++;
			}
			catch (ArgumentException)
			{
				throw new HadoopIllegalArgumentException("Invalid type of acl in <aclSpec> :" + aclStr
					);
			}
			if (split.Length > index)
			{
				string name = split[index];
				if (!name.IsEmpty())
				{
					builder.SetName(name);
				}
				index++;
			}
			if (includePermission)
			{
				if (split.Length <= index)
				{
					throw new HadoopIllegalArgumentException("Invalid <aclSpec> : " + aclStr);
				}
				string permission = split[index];
				FsAction fsAction = FsAction.GetFsAction(permission);
				if (null == fsAction)
				{
					throw new HadoopIllegalArgumentException("Invalid permission in <aclSpec> : " + aclStr
						);
				}
				builder.SetPermission(fsAction);
				index++;
			}
			if (split.Length > index)
			{
				throw new HadoopIllegalArgumentException("Invalid <aclSpec> : " + aclStr);
			}
			AclEntry aclEntry = builder.Build();
			return aclEntry;
		}

		/// <summary>Convert a List of AclEntries into a string - the reverse of parseAclSpec.
		/// 	</summary>
		/// <param name="aclSpec">List of AclEntries to convert</param>
		/// <returns>String representation of aclSpec</returns>
		public static string AclSpecToString(IList<AclEntry> aclSpec)
		{
			StringBuilder buf = new StringBuilder();
			foreach (AclEntry e in aclSpec)
			{
				buf.Append(e.ToString());
				buf.Append(",");
			}
			return buf.Substring(0, buf.Length - 1);
		}
		// remove last ,
	}
}
