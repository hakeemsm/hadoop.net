using Sharpen;

namespace org.apache.hadoop.fs.permission
{
	/// <summary>An AclStatus contains the ACL information of a specific file.</summary>
	/// <remarks>
	/// An AclStatus contains the ACL information of a specific file. AclStatus
	/// instances are immutable. Use a
	/// <see cref="Builder"/>
	/// to create a new instance.
	/// </remarks>
	public class AclStatus
	{
		private readonly string owner;

		private readonly string group;

		private readonly bool stickyBit;

		private readonly System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
			> entries;

		private readonly org.apache.hadoop.fs.permission.FsPermission permission;

		/// <summary>Returns the file owner.</summary>
		/// <returns>String file owner</returns>
		public virtual string getOwner()
		{
			return owner;
		}

		/// <summary>Returns the file group.</summary>
		/// <returns>String file group</returns>
		public virtual string getGroup()
		{
			return group;
		}

		/// <summary>Returns the sticky bit.</summary>
		/// <returns>boolean sticky bit</returns>
		public virtual bool isStickyBit()
		{
			return stickyBit;
		}

		/// <summary>Returns the list of all ACL entries, ordered by their natural ordering.</summary>
		/// <returns>List<AclEntry> unmodifiable ordered list of all ACL entries</returns>
		public virtual System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
			> getEntries()
		{
			return entries;
		}

		/// <summary>Returns the permission set for the path</summary>
		/// <returns>
		/// 
		/// <see cref="FsPermission"/>
		/// for the path
		/// </returns>
		public virtual org.apache.hadoop.fs.permission.FsPermission getPermission()
		{
			return permission;
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
			org.apache.hadoop.fs.permission.AclStatus other = (org.apache.hadoop.fs.permission.AclStatus
				)o;
			return com.google.common.@base.Objects.equal(owner, other.owner) && com.google.common.@base.Objects
				.equal(group, other.group) && stickyBit == other.stickyBit && com.google.common.@base.Objects
				.equal(entries, other.entries);
		}

		public override int GetHashCode()
		{
			return com.google.common.@base.Objects.hashCode(owner, group, stickyBit, entries);
		}

		public override string ToString()
		{
			return new java.lang.StringBuilder().Append("owner: ").Append(owner).Append(", group: "
				).Append(group).Append(", acl: {").Append("entries: ").Append(entries).Append(", stickyBit: "
				).Append(stickyBit).Append('}').ToString();
		}

		/// <summary>Builder for creating new Acl instances.</summary>
		public class Builder
		{
			private string owner;

			private string group;

			private bool stickyBit;

			private System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
				> entries = com.google.common.collect.Lists.newArrayList();

			private org.apache.hadoop.fs.permission.FsPermission permission = null;

			/// <summary>Sets the file owner.</summary>
			/// <param name="owner">String file owner</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual org.apache.hadoop.fs.permission.AclStatus.Builder owner(string owner
				)
			{
				this.owner = owner;
				return this;
			}

			/// <summary>Sets the file group.</summary>
			/// <param name="group">String file group</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual org.apache.hadoop.fs.permission.AclStatus.Builder group(string group
				)
			{
				this.group = group;
				return this;
			}

			/// <summary>Adds an ACL entry.</summary>
			/// <param name="e">AclEntry entry to add</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual org.apache.hadoop.fs.permission.AclStatus.Builder addEntry(org.apache.hadoop.fs.permission.AclEntry
				 e)
			{
				this.entries.add(e);
				return this;
			}

			/// <summary>Adds a list of ACL entries.</summary>
			/// <param name="entries">AclEntry entries to add</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual org.apache.hadoop.fs.permission.AclStatus.Builder addEntries(System.Collections.Generic.IEnumerable
				<org.apache.hadoop.fs.permission.AclEntry> entries)
			{
				foreach (org.apache.hadoop.fs.permission.AclEntry e in entries)
				{
					this.entries.add(e);
				}
				return this;
			}

			/// <summary>Sets sticky bit.</summary>
			/// <remarks>
			/// Sets sticky bit. If this method is not called, then the builder assumes
			/// false.
			/// </remarks>
			/// <param name="stickyBit">boolean sticky bit</param>
			/// <returns>Builder this builder, for call chaining</returns>
			public virtual org.apache.hadoop.fs.permission.AclStatus.Builder stickyBit(bool stickyBit
				)
			{
				this.stickyBit = stickyBit;
				return this;
			}

			/// <summary>Sets the permission for the file.</summary>
			/// <param name="permission"/>
			public virtual org.apache.hadoop.fs.permission.AclStatus.Builder setPermission(org.apache.hadoop.fs.permission.FsPermission
				 permission)
			{
				this.permission = permission;
				return this;
			}

			/// <summary>Builds a new AclStatus populated with the set properties.</summary>
			/// <returns>AclStatus new AclStatus</returns>
			public virtual org.apache.hadoop.fs.permission.AclStatus build()
			{
				return new org.apache.hadoop.fs.permission.AclStatus(owner, group, stickyBit, entries
					, permission);
			}
		}

		/// <summary>Private constructor.</summary>
		/// <param name="file">Path file associated to this ACL</param>
		/// <param name="owner">String file owner</param>
		/// <param name="group">String file group</param>
		/// <param name="stickyBit">the sticky bit</param>
		/// <param name="entries">the ACL entries</param>
		/// <param name="permission">permission of the path</param>
		private AclStatus(string owner, string group, bool stickyBit, System.Collections.Generic.IEnumerable
			<org.apache.hadoop.fs.permission.AclEntry> entries, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			this.owner = owner;
			this.group = group;
			this.stickyBit = stickyBit;
			this.entries = com.google.common.collect.Lists.newArrayList(entries);
			this.permission = permission;
		}

		/// <summary>Get the effective permission for the AclEntry</summary>
		/// <param name="entry">AclEntry to get the effective action</param>
		public virtual org.apache.hadoop.fs.permission.FsAction getEffectivePermission(org.apache.hadoop.fs.permission.AclEntry
			 entry)
		{
			return getEffectivePermission(entry, permission);
		}

		/// <summary>Get the effective permission for the AclEntry.</summary>
		/// <remarks>
		/// Get the effective permission for the AclEntry. <br />
		/// Recommended to use this API ONLY if client communicates with the old
		/// NameNode, needs to pass the Permission for the path to get effective
		/// permission, else use
		/// <see cref="getEffectivePermission(AclEntry)"/>
		/// .
		/// </remarks>
		/// <param name="entry">AclEntry to get the effective action</param>
		/// <param name="permArg">
		/// Permission for the path. However if the client is NOT
		/// communicating with old namenode, then this argument will not have
		/// any preference.
		/// </param>
		/// <returns>Returns the effective permission for the entry.</returns>
		/// <exception cref="System.ArgumentException">
		/// If the client communicating with old
		/// namenode and permission is not passed as an argument.
		/// </exception>
		public virtual org.apache.hadoop.fs.permission.FsAction getEffectivePermission(org.apache.hadoop.fs.permission.AclEntry
			 entry, org.apache.hadoop.fs.permission.FsPermission permArg)
		{
			// At least one permission bits should be available.
			com.google.common.@base.Preconditions.checkArgument(this.permission != null || permArg
				 != null, "Permission bits are not available to calculate effective permission");
			if (this.permission != null)
			{
				// permission bits from server response will have the priority for
				// accuracy.
				permArg = this.permission;
			}
			if ((entry.getName() != null || entry.getType() == org.apache.hadoop.fs.permission.AclEntryType
				.GROUP))
			{
				if (entry.getScope() == org.apache.hadoop.fs.permission.AclEntryScope.ACCESS)
				{
					org.apache.hadoop.fs.permission.FsAction entryPerm = entry.getPermission();
					return entryPerm.and(permArg.getGroupAction());
				}
				else
				{
					com.google.common.@base.Preconditions.checkArgument(this.entries.contains(entry) 
						&& this.entries.Count >= 3, "Passed default ACL entry not found in the list of ACLs"
						);
					// default mask entry for effective permission calculation will be the
					// penultimate entry. This can be mask entry in case of extended ACLs.
					// In case of minimal ACL, this is the owner group entry, and we end up
					// intersecting group FsAction with itself, which is a no-op.
					org.apache.hadoop.fs.permission.FsAction defaultMask = this.entries[this.entries.
						Count - 2].getPermission();
					org.apache.hadoop.fs.permission.FsAction entryPerm = entry.getPermission();
					return entryPerm.and(defaultMask);
				}
			}
			else
			{
				return entry.getPermission();
			}
		}
	}
}
