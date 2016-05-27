/*
* Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>
	/// Options for contract tests: keys for FS-specific values,
	/// defaults.
	/// </summary>
	public abstract class ContractOptions
	{
		/// <summary>
		/// name of the (optional) resource containing filesystem binding keys :
		/// <value/>
		/// If found, it it will be loaded
		/// </summary>
		public const string ContractOptionsResource = "contract-test-options.xml";

		/// <summary>Prefix for all contract keys in the configuration files</summary>
		public const string FsContractKey = "fs.contract.";

		/// <summary>Is a filesystem case sensitive.</summary>
		/// <remarks>
		/// Is a filesystem case sensitive.
		/// Some of the filesystems that say "no" here may mean
		/// that it varies from platform to platform -the localfs being the key
		/// example.
		/// </remarks>
		public const string IsCaseSensitive = "is-case-sensitive";

		/// <summary>Blobstore flag.</summary>
		/// <remarks>
		/// Blobstore flag. Implies it's not a real directory tree and
		/// consistency is below that which Hadoop expects
		/// </remarks>
		public const string IsBlobstore = "is-blobstore";

		/// <summary>
		/// Flag to indicate that the FS can rename into directories that
		/// don't exist, creating them as needed.
		/// </summary>
		/// <{value>}</{value>
		public const string RenameCreatesDestDirs = "rename-creates-dest-dirs";

		/// <summary>
		/// Flag to indicate that the FS does not follow the rename contract -and
		/// instead only returns false on a failure.
		/// </summary>
		/// <{value>}</{value>
		public const string RenameOverwritesDest = "rename-overwrites-dest";

		/// <summary>Flag to indicate that the FS returns false if the destination exists</summary>
		/// <{value>}</{value>
		public const string RenameReturnsFalseIfDestExists = "rename-returns-false-if-dest-exists";

		/// <summary>
		/// Flag to indicate that the FS returns false on a rename
		/// if the source is missing
		/// </summary>
		/// <{value>}</{value>
		public const string RenameReturnsFalseIfSourceMissing = "rename-returns-false-if-source-missing";

		/// <summary>
		/// Flag to indicate that the FS remove dest first if it is an empty directory
		/// mean the FS honors POSIX rename behavior.
		/// </summary>
		/// <{value>}</{value>
		public const string RenameRemoveDestIfEmptyDir = "rename-remove-dest-if-empty-dir";

		/// <summary>Flag to indicate that append is supported</summary>
		/// <{value>}</{value>
		public const string SupportsAppend = "supports-append";

		/// <summary>Flag to indicate that renames are atomic</summary>
		/// <{value>}</{value>
		public const string SupportsAtomicRename = "supports-atomic-rename";

		/// <summary>Flag to indicate that directory deletes are atomic</summary>
		/// <{value>}</{value>
		public const string SupportsAtomicDirectoryDelete = "supports-atomic-directory-delete";

		/// <summary>Does the FS support multiple block locations?</summary>
		/// <{value>}</{value>
		public const string SupportsBlockLocality = "supports-block-locality";

		/// <summary>Does the FS support the concat() operation?</summary>
		/// <{value>}</{value>
		public const string SupportsConcat = "supports-concat";

		/// <summary>Is seeking supported at all?</summary>
		/// <{value>}</{value>
		public const string SupportsSeek = "supports-seek";

		/// <summary>Is seeking past the EOF allowed?</summary>
		/// <{value>}</{value>
		public const string RejectsSeekPastEof = "rejects-seek-past-eof";

		/// <summary>
		/// Is seeking on a closed file supported? Some filesystems only raise an
		/// exception later, when trying to read.
		/// </summary>
		/// <{value>}</{value>
		public const string SupportsSeekOnClosedFile = "supports-seek-on-closed-file";

		/// <summary>
		/// Flag to indicate that this FS expects to throw the strictest
		/// exceptions it can, not generic IOEs, which, if returned,
		/// must be rejected.
		/// </summary>
		/// <{value>}</{value>
		public const string SupportsStrictExceptions = "supports-strict-exceptions";

		/// <summary>Are unix permissions</summary>
		/// <{value>}</{value>
		public const string SupportsUnixPermissions = "supports-unix-permissions";

		/// <summary>Maximum path length</summary>
		/// <{value>}</{value>
		public const string MaxPath = "max-path";

		/// <summary>Maximum filesize: 0 or -1 for no limit</summary>
		/// <{value>}</{value>
		public const string MaxFilesize = "max-filesize";

		/// <summary>
		/// Flag to indicate that tests on the root directories of a filesystem/
		/// object store are permitted
		/// </summary>
		/// <{value>}</{value>
		public const string TestRootTestsEnabled = "test.root-tests-enabled";

		/// <summary>Limit for #of random seeks to perform.</summary>
		/// <remarks>
		/// Limit for #of random seeks to perform.
		/// Keep low for remote filesystems for faster tests
		/// </remarks>
		public const string TestRandomSeekCount = "test.random-seek-count";
	}

	public static class ContractOptionsConstants
	{
	}
}
