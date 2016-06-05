using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>AclTransformation defines the operations that can modify an ACL.</summary>
	/// <remarks>
	/// AclTransformation defines the operations that can modify an ACL.  All ACL
	/// modifications take as input an existing ACL and apply logic to add new
	/// entries, modify existing entries or remove old entries.  Some operations also
	/// accept an ACL spec: a list of entries that further describes the requested
	/// change.  Different operations interpret the ACL spec differently.  In the
	/// case of adding an ACL to an inode that previously did not have one, the
	/// existing ACL can be a "minimal ACL" containing exactly 3 entries for owner,
	/// group and other, all derived from the
	/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission"/>
	/// bits.
	/// The algorithms implemented here require sorted lists of ACL entries.  For any
	/// existing ACL, it is assumed that the entries are sorted.  This is because all
	/// ACL creation and modification is intended to go through these methods, and
	/// they all guarantee correct sort order in their outputs.  However, an ACL spec
	/// is considered untrusted user input, so all operations pre-sort the ACL spec as
	/// the first step.
	/// </remarks>
	internal sealed class AclTransformation
	{
		private const int MaxEntries = 32;

		/// <summary>
		/// Filters (discards) any existing ACL entries that have the same scope, type
		/// and name of any entry in the ACL spec.
		/// </summary>
		/// <remarks>
		/// Filters (discards) any existing ACL entries that have the same scope, type
		/// and name of any entry in the ACL spec.  If necessary, recalculates the mask
		/// entries.  If necessary, default entries may be inferred by copying the
		/// permissions of the corresponding access entries.  It is invalid to request
		/// removal of the mask entry from an ACL that would otherwise require a mask
		/// entry, due to existing named entries or an unnamed group entry.
		/// </remarks>
		/// <param name="existingAcl">List<AclEntry> existing ACL</param>
		/// <param name="inAclSpec">List<AclEntry> ACL spec describing entries to filter</param>
		/// <returns>List<AclEntry> new ACL</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException">if validation fails
		/// 	</exception>
		public static IList<AclEntry> FilterAclEntriesByAclSpec(IList<AclEntry> existingAcl
			, IList<AclEntry> inAclSpec)
		{
			AclTransformation.ValidatedAclSpec aclSpec = new AclTransformation.ValidatedAclSpec
				(inAclSpec);
			AList<AclEntry> aclBuilder = Lists.NewArrayListWithCapacity(MaxEntries);
			EnumMap<AclEntryScope, AclEntry> providedMask = Maps.NewEnumMap<AclEntryScope>();
			EnumSet<AclEntryScope> maskDirty = EnumSet.NoneOf<AclEntryScope>();
			EnumSet<AclEntryScope> scopeDirty = EnumSet.NoneOf<AclEntryScope>();
			foreach (AclEntry existingEntry in existingAcl)
			{
				if (aclSpec.ContainsKey(existingEntry))
				{
					scopeDirty.AddItem(existingEntry.GetScope());
					if (existingEntry.GetType() == AclEntryType.Mask)
					{
						maskDirty.AddItem(existingEntry.GetScope());
					}
				}
				else
				{
					if (existingEntry.GetType() == AclEntryType.Mask)
					{
						providedMask[existingEntry.GetScope()] = existingEntry;
					}
					else
					{
						aclBuilder.AddItem(existingEntry);
					}
				}
			}
			CopyDefaultsIfNeeded(aclBuilder);
			CalculateMasks(aclBuilder, providedMask, maskDirty, scopeDirty);
			return BuildAndValidateAcl(aclBuilder);
		}

		/// <summary>Filters (discards) any existing default ACL entries.</summary>
		/// <remarks>
		/// Filters (discards) any existing default ACL entries.  The new ACL retains
		/// only the access ACL entries.
		/// </remarks>
		/// <param name="existingAcl">List<AclEntry> existing ACL</param>
		/// <returns>List<AclEntry> new ACL</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException">if validation fails
		/// 	</exception>
		public static IList<AclEntry> FilterDefaultAclEntries(IList<AclEntry> existingAcl
			)
		{
			AList<AclEntry> aclBuilder = Lists.NewArrayListWithCapacity(MaxEntries);
			foreach (AclEntry existingEntry in existingAcl)
			{
				if (existingEntry.GetScope() == AclEntryScope.Default)
				{
					// Default entries sort after access entries, so we can exit early.
					break;
				}
				aclBuilder.AddItem(existingEntry);
			}
			return BuildAndValidateAcl(aclBuilder);
		}

		/// <summary>Merges the entries of the ACL spec into the existing ACL.</summary>
		/// <remarks>
		/// Merges the entries of the ACL spec into the existing ACL.  If necessary,
		/// recalculates the mask entries.  If necessary, default entries may be
		/// inferred by copying the permissions of the corresponding access entries.
		/// </remarks>
		/// <param name="existingAcl">List<AclEntry> existing ACL</param>
		/// <param name="inAclSpec">List<AclEntry> ACL spec containing entries to merge</param>
		/// <returns>List<AclEntry> new ACL</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException">if validation fails
		/// 	</exception>
		public static IList<AclEntry> MergeAclEntries(IList<AclEntry> existingAcl, IList<
			AclEntry> inAclSpec)
		{
			AclTransformation.ValidatedAclSpec aclSpec = new AclTransformation.ValidatedAclSpec
				(inAclSpec);
			AList<AclEntry> aclBuilder = Lists.NewArrayListWithCapacity(MaxEntries);
			IList<AclEntry> foundAclSpecEntries = Lists.NewArrayListWithCapacity(MaxEntries);
			EnumMap<AclEntryScope, AclEntry> providedMask = Maps.NewEnumMap<AclEntryScope>();
			EnumSet<AclEntryScope> maskDirty = EnumSet.NoneOf<AclEntryScope>();
			EnumSet<AclEntryScope> scopeDirty = EnumSet.NoneOf<AclEntryScope>();
			foreach (AclEntry existingEntry in existingAcl)
			{
				AclEntry aclSpecEntry = aclSpec.FindByKey(existingEntry);
				if (aclSpecEntry != null)
				{
					foundAclSpecEntries.AddItem(aclSpecEntry);
					scopeDirty.AddItem(aclSpecEntry.GetScope());
					if (aclSpecEntry.GetType() == AclEntryType.Mask)
					{
						providedMask[aclSpecEntry.GetScope()] = aclSpecEntry;
						maskDirty.AddItem(aclSpecEntry.GetScope());
					}
					else
					{
						aclBuilder.AddItem(aclSpecEntry);
					}
				}
				else
				{
					if (existingEntry.GetType() == AclEntryType.Mask)
					{
						providedMask[existingEntry.GetScope()] = existingEntry;
					}
					else
					{
						aclBuilder.AddItem(existingEntry);
					}
				}
			}
			// ACL spec entries that were not replacements are new additions.
			foreach (AclEntry newEntry in aclSpec)
			{
				if (Sharpen.Collections.BinarySearch(foundAclSpecEntries, newEntry, AclEntryComparator
					) < 0)
				{
					scopeDirty.AddItem(newEntry.GetScope());
					if (newEntry.GetType() == AclEntryType.Mask)
					{
						providedMask[newEntry.GetScope()] = newEntry;
						maskDirty.AddItem(newEntry.GetScope());
					}
					else
					{
						aclBuilder.AddItem(newEntry);
					}
				}
			}
			CopyDefaultsIfNeeded(aclBuilder);
			CalculateMasks(aclBuilder, providedMask, maskDirty, scopeDirty);
			return BuildAndValidateAcl(aclBuilder);
		}

		/// <summary>Completely replaces the ACL with the entries of the ACL spec.</summary>
		/// <remarks>
		/// Completely replaces the ACL with the entries of the ACL spec.  If
		/// necessary, recalculates the mask entries.  If necessary, default entries
		/// are inferred by copying the permissions of the corresponding access
		/// entries.  Replacement occurs separately for each of the access ACL and the
		/// default ACL.  If the ACL spec contains only access entries, then the
		/// existing default entries are retained.  If the ACL spec contains only
		/// default entries, then the existing access entries are retained.  If the ACL
		/// spec contains both access and default entries, then both are replaced.
		/// </remarks>
		/// <param name="existingAcl">List<AclEntry> existing ACL</param>
		/// <param name="inAclSpec">List<AclEntry> ACL spec containing replacement entries</param>
		/// <returns>List<AclEntry> new ACL</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException">if validation fails
		/// 	</exception>
		public static IList<AclEntry> ReplaceAclEntries(IList<AclEntry> existingAcl, IList
			<AclEntry> inAclSpec)
		{
			AclTransformation.ValidatedAclSpec aclSpec = new AclTransformation.ValidatedAclSpec
				(inAclSpec);
			AList<AclEntry> aclBuilder = Lists.NewArrayListWithCapacity(MaxEntries);
			// Replacement is done separately for each scope: access and default.
			EnumMap<AclEntryScope, AclEntry> providedMask = Maps.NewEnumMap<AclEntryScope>();
			EnumSet<AclEntryScope> maskDirty = EnumSet.NoneOf<AclEntryScope>();
			EnumSet<AclEntryScope> scopeDirty = EnumSet.NoneOf<AclEntryScope>();
			foreach (AclEntry aclSpecEntry in aclSpec)
			{
				scopeDirty.AddItem(aclSpecEntry.GetScope());
				if (aclSpecEntry.GetType() == AclEntryType.Mask)
				{
					providedMask[aclSpecEntry.GetScope()] = aclSpecEntry;
					maskDirty.AddItem(aclSpecEntry.GetScope());
				}
				else
				{
					aclBuilder.AddItem(aclSpecEntry);
				}
			}
			// Copy existing entries if the scope was not replaced.
			foreach (AclEntry existingEntry in existingAcl)
			{
				if (!scopeDirty.Contains(existingEntry.GetScope()))
				{
					if (existingEntry.GetType() == AclEntryType.Mask)
					{
						providedMask[existingEntry.GetScope()] = existingEntry;
					}
					else
					{
						aclBuilder.AddItem(existingEntry);
					}
				}
			}
			CopyDefaultsIfNeeded(aclBuilder);
			CalculateMasks(aclBuilder, providedMask, maskDirty, scopeDirty);
			return BuildAndValidateAcl(aclBuilder);
		}

		/// <summary>There is no reason to instantiate this class.</summary>
		private AclTransformation()
		{
		}

		private sealed class _IComparer_250 : IComparer<AclEntry>
		{
			public _IComparer_250()
			{
			}

			public int Compare(AclEntry entry1, AclEntry entry2)
			{
				return ComparisonChain.Start().Compare(entry1.GetScope(), entry2.GetScope(), Ordering
					.Explicit(AclEntryScope.Access, AclEntryScope.Default)).Compare(entry1.GetType()
					, entry2.GetType(), Ordering.Explicit(AclEntryType.User, AclEntryType.Group, AclEntryType
					.Mask, AclEntryType.Other)).Compare(entry1.GetName(), entry2.GetName(), Ordering
					.Natural().NullsFirst()).Result();
			}
		}

		/// <summary>
		/// Comparator that enforces required ordering for entries within an ACL:
		/// -owner entry (unnamed user)
		/// -all named user entries (internal ordering undefined)
		/// -owning group entry (unnamed group)
		/// -all named group entries (internal ordering undefined)
		/// -mask entry
		/// -other entry
		/// All access ACL entries sort ahead of all default ACL entries.
		/// </summary>
		internal static readonly IComparer<AclEntry> AclEntryComparator = new _IComparer_250
			();

		/// <summary>
		/// Builds the final list of ACL entries to return by trimming, sorting and
		/// validating the ACL entries that have been added.
		/// </summary>
		/// <param name="aclBuilder">ArrayList<AclEntry> containing entries to build</param>
		/// <returns>List<AclEntry> unmodifiable, sorted list of ACL entries</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException">if validation fails
		/// 	</exception>
		private static IList<AclEntry> BuildAndValidateAcl(AList<AclEntry> aclBuilder)
		{
			if (aclBuilder.Count > MaxEntries)
			{
				throw new AclException("Invalid ACL: ACL has " + aclBuilder.Count + " entries, which exceeds maximum of "
					 + MaxEntries + ".");
			}
			aclBuilder.TrimToSize();
			aclBuilder.Sort(AclEntryComparator);
			// Full iteration to check for duplicates and invalid named entries.
			AclEntry prevEntry = null;
			foreach (AclEntry entry in aclBuilder)
			{
				if (prevEntry != null && AclEntryComparator.Compare(prevEntry, entry) == 0)
				{
					throw new AclException("Invalid ACL: multiple entries with same scope, type and name."
						);
				}
				if (entry.GetName() != null && (entry.GetType() == AclEntryType.Mask || entry.GetType
					() == AclEntryType.Other))
				{
					throw new AclException("Invalid ACL: this entry type must not have a name: " + entry
						 + ".");
				}
				prevEntry = entry;
			}
			// Search for the required base access entries.  If there is a default ACL,
			// then do the same check on the default entries.
			ScopedAclEntries scopedEntries = new ScopedAclEntries(aclBuilder);
			foreach (AclEntryType type in EnumSet.Of(AclEntryType.User, AclEntryType.Group, AclEntryType
				.Other))
			{
				AclEntry accessEntryKey = new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType
					(type).Build();
				if (Sharpen.Collections.BinarySearch(scopedEntries.GetAccessEntries(), accessEntryKey
					, AclEntryComparator) < 0)
				{
					throw new AclException("Invalid ACL: the user, group and other entries are required."
						);
				}
				if (!scopedEntries.GetDefaultEntries().IsEmpty())
				{
					AclEntry defaultEntryKey = new AclEntry.Builder().SetScope(AclEntryScope.Default)
						.SetType(type).Build();
					if (Sharpen.Collections.BinarySearch(scopedEntries.GetDefaultEntries(), defaultEntryKey
						, AclEntryComparator) < 0)
					{
						throw new AclException("Invalid default ACL: the user, group and other entries are required."
							);
					}
				}
			}
			return Sharpen.Collections.UnmodifiableList(aclBuilder);
		}

		/// <summary>Calculates mask entries required for the ACL.</summary>
		/// <remarks>
		/// Calculates mask entries required for the ACL.  Mask calculation is performed
		/// separately for each scope: access and default.  This method is responsible
		/// for handling the following cases of mask calculation:
		/// 1. Throws an exception if the caller attempts to remove the mask entry of an
		/// existing ACL that requires it.  If the ACL has any named entries, then a
		/// mask entry is required.
		/// 2. If the caller supplied a mask in the ACL spec, use it.
		/// 3. If the caller did not supply a mask, but there are ACL entry changes in
		/// this scope, then automatically calculate a new mask.  The permissions of
		/// the new mask are the union of the permissions on the group entry and all
		/// named entries.
		/// </remarks>
		/// <param name="aclBuilder">ArrayList<AclEntry> containing entries to build</param>
		/// <param name="providedMask">
		/// EnumMap<AclEntryScope, AclEntry> mapping each scope to
		/// the mask entry that was provided for that scope (if provided)
		/// </param>
		/// <param name="maskDirty">
		/// EnumSet<AclEntryScope> which contains a scope if the mask
		/// entry is dirty (added or deleted) in that scope
		/// </param>
		/// <param name="scopeDirty">
		/// EnumSet<AclEntryScope> which contains a scope if any entry
		/// is dirty (added or deleted) in that scope
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException">if validation fails
		/// 	</exception>
		private static void CalculateMasks(IList<AclEntry> aclBuilder, EnumMap<AclEntryScope
			, AclEntry> providedMask, EnumSet<AclEntryScope> maskDirty, EnumSet<AclEntryScope
			> scopeDirty)
		{
			EnumSet<AclEntryScope> scopeFound = EnumSet.NoneOf<AclEntryScope>();
			EnumMap<AclEntryScope, FsAction> unionPerms = Maps.NewEnumMap<AclEntryScope>();
			EnumSet<AclEntryScope> maskNeeded = EnumSet.NoneOf<AclEntryScope>();
			// Determine which scopes are present, which scopes need a mask, and the
			// union of group class permissions in each scope.
			foreach (AclEntry entry in aclBuilder)
			{
				scopeFound.AddItem(entry.GetScope());
				if (entry.GetType() == AclEntryType.Group || entry.GetName() != null)
				{
					FsAction scopeUnionPerms = Objects.FirstNonNull(unionPerms[entry.GetScope()], FsAction
						.None);
					unionPerms[entry.GetScope()] = scopeUnionPerms.Or(entry.GetPermission());
				}
				if (entry.GetName() != null)
				{
					maskNeeded.AddItem(entry.GetScope());
				}
			}
			// Add mask entry if needed in each scope.
			foreach (AclEntryScope scope in scopeFound)
			{
				if (!providedMask.Contains(scope) && maskNeeded.Contains(scope) && maskDirty.Contains
					(scope))
				{
					// Caller explicitly removed mask entry, but it's required.
					throw new AclException("Invalid ACL: mask is required and cannot be deleted.");
				}
				else
				{
					if (providedMask.Contains(scope) && (!scopeDirty.Contains(scope) || maskDirty.Contains
						(scope)))
					{
						// Caller explicitly provided new mask, or we are preserving the existing
						// mask in an unchanged scope.
						aclBuilder.AddItem(providedMask[scope]);
					}
					else
					{
						if (maskNeeded.Contains(scope) || providedMask.Contains(scope))
						{
							// Otherwise, if there are maskable entries present, or the ACL
							// previously had a mask, then recalculate a mask automatically.
							aclBuilder.AddItem(new AclEntry.Builder().SetScope(scope).SetType(AclEntryType.Mask
								).SetPermission(unionPerms[scope]).Build());
						}
					}
				}
			}
		}

		/// <summary>
		/// Adds unspecified default entries by copying permissions from the
		/// corresponding access entries.
		/// </summary>
		/// <param name="aclBuilder">ArrayList<AclEntry> containing entries to build</param>
		private static void CopyDefaultsIfNeeded(IList<AclEntry> aclBuilder)
		{
			aclBuilder.Sort(AclEntryComparator);
			ScopedAclEntries scopedEntries = new ScopedAclEntries(aclBuilder);
			if (!scopedEntries.GetDefaultEntries().IsEmpty())
			{
				IList<AclEntry> accessEntries = scopedEntries.GetAccessEntries();
				IList<AclEntry> defaultEntries = scopedEntries.GetDefaultEntries();
				IList<AclEntry> copiedEntries = Lists.NewArrayListWithCapacity(3);
				foreach (AclEntryType type in EnumSet.Of(AclEntryType.User, AclEntryType.Group, AclEntryType
					.Other))
				{
					AclEntry defaultEntryKey = new AclEntry.Builder().SetScope(AclEntryScope.Default)
						.SetType(type).Build();
					int defaultEntryIndex = Sharpen.Collections.BinarySearch(defaultEntries, defaultEntryKey
						, AclEntryComparator);
					if (defaultEntryIndex < 0)
					{
						AclEntry accessEntryKey = new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType
							(type).Build();
						int accessEntryIndex = Sharpen.Collections.BinarySearch(accessEntries, accessEntryKey
							, AclEntryComparator);
						if (accessEntryIndex >= 0)
						{
							copiedEntries.AddItem(new AclEntry.Builder().SetScope(AclEntryScope.Default).SetType
								(type).SetPermission(accessEntries[accessEntryIndex].GetPermission()).Build());
						}
					}
				}
				// Add all copied entries when done to prevent potential issues with binary
				// search on a modified aclBulider during the main loop.
				Sharpen.Collections.AddAll(aclBuilder, copiedEntries);
			}
		}

		/// <summary>An ACL spec that has been pre-validated and sorted.</summary>
		private sealed class ValidatedAclSpec : IEnumerable<AclEntry>
		{
			private readonly IList<AclEntry> aclSpec;

			/// <summary>
			/// Creates a ValidatedAclSpec by pre-validating and sorting the given ACL
			/// entries.
			/// </summary>
			/// <remarks>
			/// Creates a ValidatedAclSpec by pre-validating and sorting the given ACL
			/// entries.  Pre-validation checks that it does not exceed the maximum
			/// entries.  This check is performed before modifying the ACL, and it's
			/// actually insufficient for enforcing the maximum number of entries.
			/// Transformation logic can create additional entries automatically,such as
			/// the mask and some of the default entries, so we also need additional
			/// checks during transformation.  The up-front check is still valuable here
			/// so that we don't run a lot of expensive transformation logic while
			/// holding the namesystem lock for an attacker who intentionally sent a huge
			/// ACL spec.
			/// </remarks>
			/// <param name="aclSpec">List<AclEntry> containing unvalidated input ACL spec</param>
			/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException">if validation fails
			/// 	</exception>
			public ValidatedAclSpec(IList<AclEntry> aclSpec)
			{
				if (aclSpec.Count > MaxEntries)
				{
					throw new AclException("Invalid ACL: ACL spec has " + aclSpec.Count + " entries, which exceeds maximum of "
						 + MaxEntries + ".");
				}
				aclSpec.Sort(AclEntryComparator);
				this.aclSpec = aclSpec;
			}

			/// <summary>Returns true if this contains an entry matching the given key.</summary>
			/// <remarks>
			/// Returns true if this contains an entry matching the given key.  An ACL
			/// entry's key consists of scope, type and name (but not permission).
			/// </remarks>
			/// <param name="key">AclEntry search key</param>
			/// <returns>boolean true if found</returns>
			public bool ContainsKey(AclEntry key)
			{
				return Sharpen.Collections.BinarySearch(aclSpec, key, AclEntryComparator) >= 0;
			}

			/// <summary>Returns the entry matching the given key or null if not found.</summary>
			/// <remarks>
			/// Returns the entry matching the given key or null if not found.  An ACL
			/// entry's key consists of scope, type and name (but not permission).
			/// </remarks>
			/// <param name="key">AclEntry search key</param>
			/// <returns>AclEntry entry matching the given key or null if not found</returns>
			public AclEntry FindByKey(AclEntry key)
			{
				int index = Sharpen.Collections.BinarySearch(aclSpec, key, AclEntryComparator);
				if (index >= 0)
				{
					return aclSpec[index];
				}
				return null;
			}

			public override IEnumerator<AclEntry> GetEnumerator()
			{
				return aclSpec.GetEnumerator();
			}
		}
	}
}
