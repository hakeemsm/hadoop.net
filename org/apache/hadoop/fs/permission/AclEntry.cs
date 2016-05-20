using Sharpen;

namespace org.apache.hadoop.fs.permission
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
		private readonly org.apache.hadoop.fs.permission.AclEntryType type;

		private readonly string name;

		private readonly org.apache.hadoop.fs.permission.FsAction permission;

		private readonly org.apache.hadoop.fs.permission.AclEntryScope scope;

		/// <summary>Returns the ACL entry type.</summary>
		/// <returns>AclEntryType ACL entry type</returns>
		public virtual org.apache.hadoop.fs.permission.AclEntryType getType()
		{
			return type;
		}

		/// <summary>Returns the optional ACL entry name.</summary>
		/// <returns>String ACL entry name, or null if undefined</returns>
		public virtual string getName()
		{
			return name;
		}

		/// <summary>Returns the set of permissions in the ACL entry.</summary>
		/// <returns>FsAction set of permissions in the ACL entry</returns>
		public virtual org.apache.hadoop.fs.permission.FsAction getPermission()
		{
			return permission;
		}

		/// <summary>Returns the scope of the ACL entry.</summary>
		/// <returns>AclEntryScope scope of the ACL entry</returns>
		public virtual org.apache.hadoop.fs.permission.AclEntryScope getScope()
		{
			return scope;
		}

		public override bool Equals(object o)
		{
			if (o == null)
			{
				return false;
			}
			if (Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject(
				o))
			{
				return false;
			}
			org.apache.hadoop.fs.permission.AclEntry other = (org.apache.hadoop.fs.permission.AclEntry
				)o;
			return com.google.common.@base.Objects.equal(type, other.type) && com.google.common.@base.Objects
				.equal(name, other.name) && com.google.common.@base.Objects.equal(permission, other
				.permission) && com.google.common.@base.Objects.equal(scope, other.scope);
		}

		public override int GetHashCode()
		{
			return com.google.common.@base.Objects.hashCode(type, name, permission, scope);
		}

		public override string ToString()
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder();
			if (scope == org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT)
			{
				sb.Append("default:");
			}
			if (type != null)
			{
				sb.Append(org.apache.hadoop.util.StringUtils.toLowerCase(type.ToString()));
			}
			sb.Append(':');
			if (name != null)
			{
				sb.Append(name);
			}
			sb.Append(':');
			if (permission != null)
			{
				sb.Append(permission.SYMBOL);
			}
			return sb.ToString();
		}

		/// <summary>Builder for creating new AclEntry instances.</summary>
		public class Builder
		{
			private org.apache.hadoop.fs.permission.AclEntryType type;

			private string name;

			private org.apache.hadoop.fs.permission.FsAction permission;

			private org.apache.hadoop.fs.permission.AclEntryScope scope = org.apache.hadoop.fs.permission.AclEntryScope
				.ACCESS;

			/// <summary>Sets the ACL entry type.</summary>
			/// <param name="type">AclEntryType ACL entry type</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual org.apache.hadoop.fs.permission.AclEntry.Builder setType(org.apache.hadoop.fs.permission.AclEntryType
				 type)
			{
				this.type = type;
				return this;
			}

			/// <summary>Sets the optional ACL entry name.</summary>
			/// <param name="name">String optional ACL entry name</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual org.apache.hadoop.fs.permission.AclEntry.Builder setName(string name
				)
			{
				if (name != null && !name.isEmpty())
				{
					this.name = name;
				}
				return this;
			}

			/// <summary>Sets the set of permissions in the ACL entry.</summary>
			/// <param name="permission">FsAction set of permissions in the ACL entry</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual org.apache.hadoop.fs.permission.AclEntry.Builder setPermission(org.apache.hadoop.fs.permission.FsAction
				 permission)
			{
				this.permission = permission;
				return this;
			}

			/// <summary>Sets the scope of the ACL entry.</summary>
			/// <remarks>
			/// Sets the scope of the ACL entry.  If this method is not called, then the
			/// builder assumes
			/// <see cref="AclEntryScope.ACCESS"/>
			/// .
			/// </remarks>
			/// <param name="scope">AclEntryScope scope of the ACL entry</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual org.apache.hadoop.fs.permission.AclEntry.Builder setScope(org.apache.hadoop.fs.permission.AclEntryScope
				 scope)
			{
				this.scope = scope;
				return this;
			}

			/// <summary>Builds a new AclEntry populated with the set properties.</summary>
			/// <returns>AclEntry new AclEntry</returns>
			public virtual org.apache.hadoop.fs.permission.AclEntry build()
			{
				return new org.apache.hadoop.fs.permission.AclEntry(type, name, permission, scope
					);
			}
		}

		/// <summary>Private constructor.</summary>
		/// <param name="type">AclEntryType ACL entry type</param>
		/// <param name="name">String optional ACL entry name</param>
		/// <param name="permission">FsAction set of permissions in the ACL entry</param>
		/// <param name="scope">AclEntryScope scope of the ACL entry</param>
		private AclEntry(org.apache.hadoop.fs.permission.AclEntryType type, string name, 
			org.apache.hadoop.fs.permission.FsAction permission, org.apache.hadoop.fs.permission.AclEntryScope
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
		public static System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
			> parseAclSpec(string aclSpec, bool includePermission)
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> aclEntries
				 = new System.Collections.Generic.List<org.apache.hadoop.fs.permission.AclEntry>
				();
			System.Collections.Generic.ICollection<string> aclStrings = org.apache.hadoop.util.StringUtils
				.getStringCollection(aclSpec, ",");
			foreach (string aclStr in aclStrings)
			{
				org.apache.hadoop.fs.permission.AclEntry aclEntry = parseAclEntry(aclStr, includePermission
					);
				aclEntries.add(aclEntry);
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
		public static org.apache.hadoop.fs.permission.AclEntry parseAclEntry(string aclStr
			, bool includePermission)
		{
			org.apache.hadoop.fs.permission.AclEntry.Builder builder = new org.apache.hadoop.fs.permission.AclEntry.Builder
				();
			// Here "::" represent one empty string.
			// StringUtils.getStringCollection() will ignore this.
			string[] split = aclStr.split(":");
			if (split.Length == 0)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Invalid <aclSpec> : "
					 + aclStr);
			}
			int index = 0;
			if ("default".Equals(split[0]))
			{
				// default entry
				index++;
				builder.setScope(org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT);
			}
			if (split.Length <= index)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Invalid <aclSpec> : "
					 + aclStr);
			}
			org.apache.hadoop.fs.permission.AclEntryType aclType = null;
			try
			{
				aclType = java.lang.Enum.valueOf<org.apache.hadoop.fs.permission.AclEntryType>(org.apache.hadoop.util.StringUtils
					.toUpperCase(split[index]));
				builder.setType(aclType);
				index++;
			}
			catch (System.ArgumentException)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Invalid type of acl in <aclSpec> :"
					 + aclStr);
			}
			if (split.Length > index)
			{
				string name = split[index];
				if (!name.isEmpty())
				{
					builder.setName(name);
				}
				index++;
			}
			if (includePermission)
			{
				if (split.Length <= index)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Invalid <aclSpec> : "
						 + aclStr);
				}
				string permission = split[index];
				org.apache.hadoop.fs.permission.FsAction fsAction = org.apache.hadoop.fs.permission.FsAction
					.getFsAction(permission);
				if (null == fsAction)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Invalid permission in <aclSpec> : "
						 + aclStr);
				}
				builder.setPermission(fsAction);
				index++;
			}
			if (split.Length > index)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Invalid <aclSpec> : "
					 + aclStr);
			}
			org.apache.hadoop.fs.permission.AclEntry aclEntry = builder.build();
			return aclEntry;
		}

		/// <summary>Convert a List of AclEntries into a string - the reverse of parseAclSpec.
		/// 	</summary>
		/// <param name="aclSpec">List of AclEntries to convert</param>
		/// <returns>String representation of aclSpec</returns>
		public static string aclSpecToString(System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
			> aclSpec)
		{
			java.lang.StringBuilder buf = new java.lang.StringBuilder();
			foreach (org.apache.hadoop.fs.permission.AclEntry e in aclSpec)
			{
				buf.Append(e.ToString());
				buf.Append(",");
			}
			return buf.substring(0, buf.Length - 1);
		}
		// remove last ,
	}
}
