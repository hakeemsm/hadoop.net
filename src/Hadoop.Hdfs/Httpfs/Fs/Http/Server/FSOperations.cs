using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Http.Client;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Util;
using Org.Json.Simple;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	/// <summary>
	/// FileSystem operation executors used by
	/// <see cref="HttpFSServer"/>
	/// .
	/// </summary>
	public class FSOperations
	{
		/// <summary>This class is used to group a FileStatus and an AclStatus together.</summary>
		/// <remarks>
		/// This class is used to group a FileStatus and an AclStatus together.
		/// It's needed for the GETFILESTATUS and LISTSTATUS calls, which take
		/// most info from the FileStatus and a wee bit from the AclStatus.
		/// </remarks>
		private class StatusPair
		{
			private FileStatus fileStatus;

			private AclStatus aclStatus;

			/// <summary>Simple constructor</summary>
			/// <param name="fileStatus">Existing FileStatus object</param>
			/// <param name="aclStatus">Existing AclStatus object</param>
			public StatusPair(FileStatus fileStatus, AclStatus aclStatus)
			{
				this.fileStatus = fileStatus;
				this.aclStatus = aclStatus;
			}

			/// <summary>
			/// Create one StatusPair by performing the underlying calls to
			/// fs.getFileStatus and fs.getAclStatus
			/// </summary>
			/// <param name="fs">The FileSystem where 'path' lives</param>
			/// <param name="path">The file/directory to query</param>
			/// <exception cref="System.IO.IOException"/>
			public StatusPair(FileSystem fs, Path path)
			{
				fileStatus = fs.GetFileStatus(path);
				aclStatus = null;
				try
				{
					aclStatus = fs.GetAclStatus(path);
				}
				catch (AclException)
				{
				}
				catch (NotSupportedException)
				{
				}
			}

			/*
			* The cause is almost certainly an "ACLS aren't enabled"
			* exception, so leave aclStatus at null and carry on.
			*/
			/* Ditto above - this is the case for a local file system */
			/// <summary>Return a Map suitable for conversion into JSON format</summary>
			/// <returns>The JSONish Map</returns>
			public virtual IDictionary<string, object> ToJson()
			{
				IDictionary<string, object> json = new LinkedHashMap<string, object>();
				json[HttpFSFileSystem.FileStatusJson] = ToJsonInner(true);
				return json;
			}

			/// <summary>
			/// Return in inner part of the JSON for the status - used by both the
			/// GETFILESTATUS and LISTSTATUS calls.
			/// </summary>
			/// <param name="emptyPathSuffix">Whether or not to include PATH_SUFFIX_JSON</param>
			/// <returns>The JSONish Map</returns>
			public virtual IDictionary<string, object> ToJsonInner(bool emptyPathSuffix)
			{
				IDictionary<string, object> json = new LinkedHashMap<string, object>();
				json[HttpFSFileSystem.PathSuffixJson] = (emptyPathSuffix) ? string.Empty : fileStatus
					.GetPath().GetName();
				json[HttpFSFileSystem.TypeJson] = HttpFSFileSystem.FILE_TYPE.GetType(fileStatus).
					ToString();
				json[HttpFSFileSystem.LengthJson] = fileStatus.GetLen();
				json[HttpFSFileSystem.OwnerJson] = fileStatus.GetOwner();
				json[HttpFSFileSystem.GroupJson] = fileStatus.GetGroup();
				json[HttpFSFileSystem.PermissionJson] = HttpFSFileSystem.PermissionToString(fileStatus
					.GetPermission());
				json[HttpFSFileSystem.AccessTimeJson] = fileStatus.GetAccessTime();
				json[HttpFSFileSystem.ModificationTimeJson] = fileStatus.GetModificationTime();
				json[HttpFSFileSystem.BlockSizeJson] = fileStatus.GetBlockSize();
				json[HttpFSFileSystem.ReplicationJson] = fileStatus.GetReplication();
				if ((aclStatus != null) && !(aclStatus.GetEntries().IsEmpty()))
				{
					json[HttpFSFileSystem.AclBitJson] = true;
				}
				return json;
			}
		}

		/// <summary>
		/// Simple class used to contain and operate upon a list of StatusPair
		/// objects.
		/// </summary>
		/// <remarks>
		/// Simple class used to contain and operate upon a list of StatusPair
		/// objects.  Used by LISTSTATUS.
		/// </remarks>
		private class StatusPairs
		{
			private FSOperations.StatusPair[] statusPairs;

			/// <summary>Construct a list of StatusPair objects</summary>
			/// <param name="fs">The FileSystem where 'path' lives</param>
			/// <param name="path">The directory to query</param>
			/// <param name="filter">A possible filter for entries in the directory</param>
			/// <exception cref="System.IO.IOException"/>
			public StatusPairs(FileSystem fs, Path path, PathFilter filter)
			{
				/* Grab all the file statuses at once in an array */
				FileStatus[] fileStatuses = fs.ListStatus(path, filter);
				/* We'll have an array of StatusPairs of the same length */
				AclStatus aclStatus = null;
				statusPairs = new FSOperations.StatusPair[fileStatuses.Length];
				/*
				* For each FileStatus, attempt to acquire an AclStatus.  If the
				* getAclStatus throws an exception, we assume that ACLs are turned
				* off entirely and abandon the attempt.
				*/
				bool useAcls = true;
				// Assume ACLs work until proven otherwise
				for (int i = 0; i < fileStatuses.Length; i++)
				{
					if (useAcls)
					{
						try
						{
							aclStatus = fs.GetAclStatus(fileStatuses[i].GetPath());
						}
						catch (AclException)
						{
							/* Almost certainly due to an "ACLs not enabled" exception */
							aclStatus = null;
							useAcls = false;
						}
						catch (NotSupportedException)
						{
							/* Ditto above - this is the case for a local file system */
							aclStatus = null;
							useAcls = false;
						}
					}
					statusPairs[i] = new FSOperations.StatusPair(fileStatuses[i], aclStatus);
				}
			}

			/// <summary>Return a Map suitable for conversion into JSON.</summary>
			/// <returns>A JSONish Map</returns>
			public virtual IDictionary<string, object> ToJson()
			{
				IDictionary<string, object> json = new LinkedHashMap<string, object>();
				IDictionary<string, object> inner = new LinkedHashMap<string, object>();
				JSONArray statuses = new JSONArray();
				foreach (FSOperations.StatusPair s in statusPairs)
				{
					statuses.AddItem(s.ToJsonInner(false));
				}
				inner[HttpFSFileSystem.FileStatusJson] = statuses;
				json[HttpFSFileSystem.FileStatusesJson] = inner;
				return json;
			}
		}

		/// <summary>Converts an <code>AclStatus</code> object into a JSON object.</summary>
		/// <param name="aclStatus">AclStatus object</param>
		/// <returns>The JSON representation of the ACLs for the file</returns>
		private static IDictionary<string, object> AclStatusToJSON(AclStatus aclStatus)
		{
			IDictionary<string, object> json = new LinkedHashMap<string, object>();
			IDictionary<string, object> inner = new LinkedHashMap<string, object>();
			JSONArray entriesArray = new JSONArray();
			inner[HttpFSFileSystem.OwnerJson] = aclStatus.GetOwner();
			inner[HttpFSFileSystem.GroupJson] = aclStatus.GetGroup();
			inner[HttpFSFileSystem.AclStickyBitJson] = aclStatus.IsStickyBit();
			foreach (AclEntry e in aclStatus.GetEntries())
			{
				entriesArray.AddItem(e.ToString());
			}
			inner[HttpFSFileSystem.AclEntriesJson] = entriesArray;
			json[HttpFSFileSystem.AclStatusJson] = inner;
			return json;
		}

		/// <summary>
		/// Converts a <code>FileChecksum</code> object into a JSON array
		/// object.
		/// </summary>
		/// <param name="checksum">file checksum.</param>
		/// <returns>The JSON representation of the file checksum.</returns>
		private static IDictionary FileChecksumToJSON(FileChecksum checksum)
		{
			IDictionary json = new LinkedHashMap();
			json[HttpFSFileSystem.ChecksumAlgorithmJson] = checksum.GetAlgorithmName();
			json[HttpFSFileSystem.ChecksumBytesJson] = StringUtils.ByteToHexString(checksum.GetBytes
				());
			json[HttpFSFileSystem.ChecksumLengthJson] = checksum.GetLength();
			IDictionary response = new LinkedHashMap();
			response[HttpFSFileSystem.FileChecksumJson] = json;
			return response;
		}

		/// <summary>Converts xAttrs to a JSON object.</summary>
		/// <param name="xAttrs">file xAttrs.</param>
		/// <param name="encoding">format of xattr values.</param>
		/// <returns>The JSON representation of the xAttrs.</returns>
		/// <exception cref="System.IO.IOException"></exception>
		private static IDictionary XAttrsToJSON(IDictionary<string, byte[]> xAttrs, XAttrCodec
			 encoding)
		{
			IDictionary jsonMap = new LinkedHashMap();
			JSONArray jsonArray = new JSONArray();
			if (xAttrs != null)
			{
				foreach (KeyValuePair<string, byte[]> e in xAttrs)
				{
					IDictionary json = new LinkedHashMap();
					json[HttpFSFileSystem.XattrNameJson] = e.Key;
					if (e.Value != null)
					{
						json[HttpFSFileSystem.XattrValueJson] = XAttrCodec.EncodeValue(e.Value, encoding);
					}
					jsonArray.AddItem(json);
				}
			}
			jsonMap[HttpFSFileSystem.XattrsJson] = jsonArray;
			return jsonMap;
		}

		/// <summary>Converts xAttr names to a JSON object.</summary>
		/// <param name="names">file xAttr names.</param>
		/// <returns>The JSON representation of the xAttr names.</returns>
		/// <exception cref="System.IO.IOException"></exception>
		private static IDictionary XAttrNamesToJSON(IList<string> names)
		{
			IDictionary jsonMap = new LinkedHashMap();
			jsonMap[HttpFSFileSystem.XattrnamesJson] = JSONArray.ToJSONString(names);
			return jsonMap;
		}

		/// <summary>
		/// Converts a <code>ContentSummary</code> object into a JSON array
		/// object.
		/// </summary>
		/// <param name="contentSummary">the content summary</param>
		/// <returns>The JSON representation of the content summary.</returns>
		private static IDictionary ContentSummaryToJSON(ContentSummary contentSummary)
		{
			IDictionary json = new LinkedHashMap();
			json[HttpFSFileSystem.ContentSummaryDirectoryCountJson] = contentSummary.GetDirectoryCount
				();
			json[HttpFSFileSystem.ContentSummaryFileCountJson] = contentSummary.GetFileCount(
				);
			json[HttpFSFileSystem.ContentSummaryLengthJson] = contentSummary.GetLength();
			json[HttpFSFileSystem.ContentSummaryQuotaJson] = contentSummary.GetQuota();
			json[HttpFSFileSystem.ContentSummarySpaceConsumedJson] = contentSummary.GetSpaceConsumed
				();
			json[HttpFSFileSystem.ContentSummarySpaceQuotaJson] = contentSummary.GetSpaceQuota
				();
			IDictionary response = new LinkedHashMap();
			response[HttpFSFileSystem.ContentSummaryJson] = json;
			return response;
		}

		/// <summary>Converts an object into a Json Map with with one key-value entry.</summary>
		/// <remarks>
		/// Converts an object into a Json Map with with one key-value entry.
		/// <p/>
		/// It assumes the given value is either a JSON primitive type or a
		/// <code>JsonAware</code> instance.
		/// </remarks>
		/// <param name="name">name for the key of the entry.</param>
		/// <param name="value">for the value of the entry.</param>
		/// <returns>the JSON representation of the key-value pair.</returns>
		private static JSONObject ToJSON(string name, object value)
		{
			JSONObject json = new JSONObject();
			json[name] = value;
			return json;
		}

		/// <summary>Executor that performs an append FileSystemAccess files system operation.
		/// 	</summary>
		public class FSAppend : FileSystemAccess.FileSystemExecutor<Void>
		{
			private InputStream @is;

			private Path path;

			/// <summary>Creates an Append executor.</summary>
			/// <param name="is">input stream to append.</param>
			/// <param name="path">path of the file to append.</param>
			public FSAppend(InputStream @is, string path)
			{
				this.@is = @is;
				this.path = new Path(path);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>void.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual Void Execute(FileSystem fs)
			{
				int bufferSize = fs.GetConf().GetInt("httpfs.buffer.size", 4096);
				OutputStream os = fs.Append(path, bufferSize);
				IOUtils.CopyBytes(@is, os, bufferSize, true);
				os.Close();
				return null;
			}
		}

		/// <summary>Executor that performs a concat FileSystemAccess files system operation.
		/// 	</summary>
		public class FSConcat : FileSystemAccess.FileSystemExecutor<Void>
		{
			private Path path;

			private Path[] sources;

			/// <summary>Creates a Concat executor.</summary>
			/// <param name="path">target path to concat to.</param>
			/// <param name="sources">comma seperated absolute paths to use as sources.</param>
			public FSConcat(string path, string[] sources)
			{
				this.sources = new Path[sources.Length];
				for (int i = 0; i < sources.Length; i++)
				{
					this.sources[i] = new Path(sources[i]);
				}
				this.path = new Path(path);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>void.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual Void Execute(FileSystem fs)
			{
				fs.Concat(path, sources);
				return null;
			}
		}

		/// <summary>Executor that performs a truncate FileSystemAccess files system operation.
		/// 	</summary>
		public class FSTruncate : FileSystemAccess.FileSystemExecutor<JSONObject>
		{
			private Path path;

			private long newLength;

			/// <summary>Creates a Truncate executor.</summary>
			/// <param name="path">target path to truncate to.</param>
			/// <param name="newLength">The size the file is to be truncated to.</param>
			public FSTruncate(string path, long newLength)
			{
				this.path = new Path(path);
				this.newLength = newLength;
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>
			/// <code>true</code> if the file has been truncated to the desired,
			/// <code>false</code> if a background process of adjusting the
			/// length of the last block has been started, and clients should
			/// wait for it to complete before proceeding with further file
			/// updates.
			/// </returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual JSONObject Execute(FileSystem fs)
			{
				bool result = fs.Truncate(path, newLength);
				return ToJSON(StringUtils.ToLowerCase(HttpFSFileSystem.TruncateJson), result);
			}
		}

		/// <summary>Executor that performs a content-summary FileSystemAccess files system operation.
		/// 	</summary>
		public class FSContentSummary : FileSystemAccess.FileSystemExecutor<IDictionary>
		{
			private Path path;

			/// <summary>Creates a content-summary executor.</summary>
			/// <param name="path">the path to retrieve the content-summary.</param>
			public FSContentSummary(string path)
			{
				this.path = new Path(path);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>a Map object (JSON friendly) with the content-summary.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual IDictionary Execute(FileSystem fs)
			{
				ContentSummary contentSummary = fs.GetContentSummary(path);
				return ContentSummaryToJSON(contentSummary);
			}
		}

		/// <summary>Executor that performs a create FileSystemAccess files system operation.
		/// 	</summary>
		public class FSCreate : FileSystemAccess.FileSystemExecutor<Void>
		{
			private InputStream @is;

			private Path path;

			private short permission;

			private bool @override;

			private short replication;

			private long blockSize;

			/// <summary>Creates a Create executor.</summary>
			/// <param name="is">input stream to for the file to create.</param>
			/// <param name="path">path of the file to create.</param>
			/// <param name="perm">permission for the file.</param>
			/// <param name="override">if the file should be overriden if it already exist.</param>
			/// <param name="repl">the replication factor for the file.</param>
			/// <param name="blockSize">the block size for the file.</param>
			public FSCreate(InputStream @is, string path, short perm, bool @override, short repl
				, long blockSize)
			{
				this.@is = @is;
				this.path = new Path(path);
				this.permission = perm;
				this.@override = @override;
				this.replication = repl;
				this.blockSize = blockSize;
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>The URI of the created file.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual Void Execute(FileSystem fs)
			{
				if (replication == -1)
				{
					replication = fs.GetDefaultReplication(path);
				}
				if (blockSize == -1)
				{
					blockSize = fs.GetDefaultBlockSize(path);
				}
				FsPermission fsPermission = new FsPermission(permission);
				int bufferSize = fs.GetConf().GetInt("httpfs.buffer.size", 4096);
				OutputStream os = fs.Create(path, fsPermission, @override, bufferSize, replication
					, blockSize, null);
				IOUtils.CopyBytes(@is, os, bufferSize, true);
				os.Close();
				return null;
			}
		}

		/// <summary>Executor that performs a delete FileSystemAccess files system operation.
		/// 	</summary>
		public class FSDelete : FileSystemAccess.FileSystemExecutor<JSONObject>
		{
			private Path path;

			private bool recursive;

			/// <summary>Creates a Delete executor.</summary>
			/// <param name="path">path to delete.</param>
			/// <param name="recursive">if the delete should be recursive or not.</param>
			public FSDelete(string path, bool recursive)
			{
				this.path = new Path(path);
				this.recursive = recursive;
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>
			/// <code>true</code> if the delete operation was successful,
			/// <code>false</code> otherwise.
			/// </returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual JSONObject Execute(FileSystem fs)
			{
				bool deleted = fs.Delete(path, recursive);
				return ToJSON(StringUtils.ToLowerCase(HttpFSFileSystem.DeleteJson), deleted);
			}
		}

		/// <summary>Executor that performs a file-checksum FileSystemAccess files system operation.
		/// 	</summary>
		public class FSFileChecksum : FileSystemAccess.FileSystemExecutor<IDictionary>
		{
			private Path path;

			/// <summary>Creates a file-checksum executor.</summary>
			/// <param name="path">the path to retrieve the checksum.</param>
			public FSFileChecksum(string path)
			{
				this.path = new Path(path);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>a Map object (JSON friendly) with the file checksum.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual IDictionary Execute(FileSystem fs)
			{
				FileChecksum checksum = fs.GetFileChecksum(path);
				return FileChecksumToJSON(checksum);
			}
		}

		/// <summary>Executor that performs a file-status FileSystemAccess files system operation.
		/// 	</summary>
		public class FSFileStatus : FileSystemAccess.FileSystemExecutor<IDictionary>
		{
			private Path path;

			/// <summary>Creates a file-status executor.</summary>
			/// <param name="path">the path to retrieve the status.</param>
			public FSFileStatus(string path)
			{
				this.path = new Path(path);
			}

			/// <summary>
			/// Executes the filesystem getFileStatus operation and returns the
			/// result in a JSONish Map.
			/// </summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>a Map object (JSON friendly) with the file status.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
			public virtual IDictionary Execute(FileSystem fs)
			{
				FSOperations.StatusPair sp = new FSOperations.StatusPair(fs, path);
				return sp.ToJson();
			}
		}

		/// <summary>Executor that performs a home-dir FileSystemAccess files system operation.
		/// 	</summary>
		public class FSHomeDir : FileSystemAccess.FileSystemExecutor<JSONObject>
		{
			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>a JSON object with the user home directory.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual JSONObject Execute(FileSystem fs)
			{
				Path homeDir = fs.GetHomeDirectory();
				JSONObject json = new JSONObject();
				json[HttpFSFileSystem.HomeDirJson] = homeDir.ToUri().GetPath();
				return json;
			}
		}

		/// <summary>Executor that performs a list-status FileSystemAccess files system operation.
		/// 	</summary>
		public class FSListStatus : FileSystemAccess.FileSystemExecutor<IDictionary>, PathFilter
		{
			private Path path;

			private PathFilter filter;

			/// <summary>Creates a list-status executor.</summary>
			/// <param name="path">the directory to retrieve the status of its contents.</param>
			/// <param name="filter">glob filter to use.</param>
			/// <exception cref="System.IO.IOException">thrown if the filter expression is incorrect.
			/// 	</exception>
			public FSListStatus(string path, string filter)
			{
				this.path = new Path(path);
				this.filter = (filter == null) ? this : new GlobFilter(filter);
			}

			/// <summary>
			/// Returns data for a JSON Map containing the information for
			/// the set of files in 'path' that match 'filter'.
			/// </summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>
			/// a Map with the file status of the directory
			/// contents that match the filter
			/// </returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
			public virtual IDictionary Execute(FileSystem fs)
			{
				FSOperations.StatusPairs sp = new FSOperations.StatusPairs(fs, path, filter);
				return sp.ToJson();
			}

			public virtual bool Accept(Path path)
			{
				return true;
			}
		}

		/// <summary>Executor that performs a mkdirs FileSystemAccess files system operation.
		/// 	</summary>
		public class FSMkdirs : FileSystemAccess.FileSystemExecutor<JSONObject>
		{
			private Path path;

			private short permission;

			/// <summary>Creates a mkdirs executor.</summary>
			/// <param name="path">directory path to create.</param>
			/// <param name="permission">permission to use.</param>
			public FSMkdirs(string path, short permission)
			{
				this.path = new Path(path);
				this.permission = permission;
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>
			/// <code>true</code> if the mkdirs operation was successful,
			/// <code>false</code> otherwise.
			/// </returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual JSONObject Execute(FileSystem fs)
			{
				FsPermission fsPermission = new FsPermission(permission);
				bool mkdirs = fs.Mkdirs(path, fsPermission);
				return ToJSON(HttpFSFileSystem.MkdirsJson, mkdirs);
			}
		}

		/// <summary>Executor that performs a open FileSystemAccess files system operation.</summary>
		public class FSOpen : FileSystemAccess.FileSystemExecutor<InputStream>
		{
			private Path path;

			/// <summary>Creates a open executor.</summary>
			/// <param name="path">file to open.</param>
			public FSOpen(string path)
			{
				this.path = new Path(path);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>The inputstream of the file.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual InputStream Execute(FileSystem fs)
			{
				int bufferSize = HttpFSServerWebApp.Get().GetConfig().GetInt("httpfs.buffer.size"
					, 4096);
				return fs.Open(path, bufferSize);
			}
		}

		/// <summary>Executor that performs a rename FileSystemAccess files system operation.
		/// 	</summary>
		public class FSRename : FileSystemAccess.FileSystemExecutor<JSONObject>
		{
			private Path path;

			private Path toPath;

			/// <summary>Creates a rename executor.</summary>
			/// <param name="path">path to rename.</param>
			/// <param name="toPath">new name.</param>
			public FSRename(string path, string toPath)
			{
				this.path = new Path(path);
				this.toPath = new Path(toPath);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>
			/// <code>true</code> if the rename operation was successful,
			/// <code>false</code> otherwise.
			/// </returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual JSONObject Execute(FileSystem fs)
			{
				bool renamed = fs.Rename(path, toPath);
				return ToJSON(HttpFSFileSystem.RenameJson, renamed);
			}
		}

		/// <summary>Executor that performs a set-owner FileSystemAccess files system operation.
		/// 	</summary>
		public class FSSetOwner : FileSystemAccess.FileSystemExecutor<Void>
		{
			private Path path;

			private string owner;

			private string group;

			/// <summary>Creates a set-owner executor.</summary>
			/// <param name="path">the path to set the owner.</param>
			/// <param name="owner">owner to set.</param>
			/// <param name="group">group to set.</param>
			public FSSetOwner(string path, string owner, string group)
			{
				this.path = new Path(path);
				this.owner = owner;
				this.group = group;
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>void.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual Void Execute(FileSystem fs)
			{
				fs.SetOwner(path, owner, group);
				return null;
			}
		}

		/// <summary>Executor that performs a set-permission FileSystemAccess files system operation.
		/// 	</summary>
		public class FSSetPermission : FileSystemAccess.FileSystemExecutor<Void>
		{
			private Path path;

			private short permission;

			/// <summary>Creates a set-permission executor.</summary>
			/// <param name="path">path to set the permission.</param>
			/// <param name="permission">permission to set.</param>
			public FSSetPermission(string path, short permission)
			{
				this.path = new Path(path);
				this.permission = permission;
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>void.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual Void Execute(FileSystem fs)
			{
				FsPermission fsPermission = new FsPermission(permission);
				fs.SetPermission(path, fsPermission);
				return null;
			}
		}

		/// <summary>Executor that sets the acl for a file in a FileSystem</summary>
		public class FSSetAcl : FileSystemAccess.FileSystemExecutor<Void>
		{
			private Path path;

			private IList<AclEntry> aclEntries;

			/// <summary>Creates a set-acl executor.</summary>
			/// <param name="path">path to set the acl.</param>
			/// <param name="aclSpec">acl to set.</param>
			public FSSetAcl(string path, string aclSpec)
			{
				this.path = new Path(path);
				this.aclEntries = AclEntry.ParseAclSpec(aclSpec, true);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>void.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
			public virtual Void Execute(FileSystem fs)
			{
				fs.SetAcl(path, aclEntries);
				return null;
			}
		}

		/// <summary>Executor that removes all acls from a file in a FileSystem</summary>
		public class FSRemoveAcl : FileSystemAccess.FileSystemExecutor<Void>
		{
			private Path path;

			/// <summary>Creates a remove-acl executor.</summary>
			/// <param name="path">path from which to remove the acl.</param>
			public FSRemoveAcl(string path)
			{
				this.path = new Path(path);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>void.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
			public virtual Void Execute(FileSystem fs)
			{
				fs.RemoveAcl(path);
				return null;
			}
		}

		/// <summary>Executor that modifies acl entries for a file in a FileSystem</summary>
		public class FSModifyAclEntries : FileSystemAccess.FileSystemExecutor<Void>
		{
			private Path path;

			private IList<AclEntry> aclEntries;

			/// <summary>Creates a modify-acl executor.</summary>
			/// <param name="path">path to set the acl.</param>
			/// <param name="aclSpec">acl to set.</param>
			public FSModifyAclEntries(string path, string aclSpec)
			{
				this.path = new Path(path);
				this.aclEntries = AclEntry.ParseAclSpec(aclSpec, true);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>void.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
			public virtual Void Execute(FileSystem fs)
			{
				fs.ModifyAclEntries(path, aclEntries);
				return null;
			}
		}

		/// <summary>Executor that removes acl entries from a file in a FileSystem</summary>
		public class FSRemoveAclEntries : FileSystemAccess.FileSystemExecutor<Void>
		{
			private Path path;

			private IList<AclEntry> aclEntries;

			/// <summary>Creates a remove acl entry executor.</summary>
			/// <param name="path">path to set the acl.</param>
			/// <param name="aclSpec">acl parts to remove.</param>
			public FSRemoveAclEntries(string path, string aclSpec)
			{
				this.path = new Path(path);
				this.aclEntries = AclEntry.ParseAclSpec(aclSpec, true);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>void.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
			public virtual Void Execute(FileSystem fs)
			{
				fs.RemoveAclEntries(path, aclEntries);
				return null;
			}
		}

		/// <summary>Executor that removes the default acl from a directory in a FileSystem</summary>
		public class FSRemoveDefaultAcl : FileSystemAccess.FileSystemExecutor<Void>
		{
			private Path path;

			/// <summary>Creates an executor for removing the default acl.</summary>
			/// <param name="path">path to set the acl.</param>
			public FSRemoveDefaultAcl(string path)
			{
				this.path = new Path(path);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>void.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
			public virtual Void Execute(FileSystem fs)
			{
				fs.RemoveDefaultAcl(path);
				return null;
			}
		}

		/// <summary>Executor that gets the ACL information for a given file.</summary>
		public class FSAclStatus : FileSystemAccess.FileSystemExecutor<IDictionary>
		{
			private Path path;

			/// <summary>Creates an executor for getting the ACLs for a file.</summary>
			/// <param name="path">the path to retrieve the ACLs.</param>
			public FSAclStatus(string path)
			{
				this.path = new Path(path);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>a Map object (JSON friendly) with the file status.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
			public virtual IDictionary Execute(FileSystem fs)
			{
				AclStatus status = fs.GetAclStatus(path);
				return AclStatusToJSON(status);
			}
		}

		/// <summary>Executor that performs a set-replication FileSystemAccess files system operation.
		/// 	</summary>
		public class FSSetReplication : FileSystemAccess.FileSystemExecutor<JSONObject>
		{
			private Path path;

			private short replication;

			/// <summary>Creates a set-replication executor.</summary>
			/// <param name="path">path to set the replication factor.</param>
			/// <param name="replication">replication factor to set.</param>
			public FSSetReplication(string path, short replication)
			{
				this.path = new Path(path);
				this.replication = replication;
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>
			/// <code>true</code> if the replication value was set,
			/// <code>false</code> otherwise.
			/// </returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual JSONObject Execute(FileSystem fs)
			{
				bool ret = fs.SetReplication(path, replication);
				JSONObject json = new JSONObject();
				json[HttpFSFileSystem.SetReplicationJson] = ret;
				return json;
			}
		}

		/// <summary>Executor that performs a set-times FileSystemAccess files system operation.
		/// 	</summary>
		public class FSSetTimes : FileSystemAccess.FileSystemExecutor<Void>
		{
			private Path path;

			private long mTime;

			private long aTime;

			/// <summary>Creates a set-times executor.</summary>
			/// <param name="path">path to set the times.</param>
			/// <param name="mTime">modified time to set.</param>
			/// <param name="aTime">access time to set.</param>
			public FSSetTimes(string path, long mTime, long aTime)
			{
				this.path = new Path(path);
				this.mTime = mTime;
				this.aTime = aTime;
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>void.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual Void Execute(FileSystem fs)
			{
				fs.SetTimes(path, mTime, aTime);
				return null;
			}
		}

		/// <summary>Executor that performs a setxattr FileSystemAccess files system operation.
		/// 	</summary>
		public class FSSetXAttr : FileSystemAccess.FileSystemExecutor<Void>
		{
			private Path path;

			private string name;

			private byte[] value;

			private EnumSet<XAttrSetFlag> flag;

			/// <exception cref="System.IO.IOException"/>
			public FSSetXAttr(string path, string name, string encodedValue, EnumSet<XAttrSetFlag
				> flag)
			{
				this.path = new Path(path);
				this.name = name;
				this.value = XAttrCodec.DecodeValue(encodedValue);
				this.flag = flag;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual Void Execute(FileSystem fs)
			{
				fs.SetXAttr(path, name, value, flag);
				return null;
			}
		}

		/// <summary>
		/// Executor that performs a removexattr FileSystemAccess files system
		/// operation.
		/// </summary>
		public class FSRemoveXAttr : FileSystemAccess.FileSystemExecutor<Void>
		{
			private Path path;

			private string name;

			public FSRemoveXAttr(string path, string name)
			{
				this.path = new Path(path);
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual Void Execute(FileSystem fs)
			{
				fs.RemoveXAttr(path, name);
				return null;
			}
		}

		/// <summary>
		/// Executor that performs listing xattrs FileSystemAccess files system
		/// operation.
		/// </summary>
		public class FSListXAttrs : FileSystemAccess.FileSystemExecutor<IDictionary>
		{
			private Path path;

			/// <summary>Creates listing xattrs executor.</summary>
			/// <param name="path">the path to retrieve the xattrs.</param>
			public FSListXAttrs(string path)
			{
				this.path = new Path(path);
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>Map a map object (JSON friendly) with the xattr names.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual IDictionary Execute(FileSystem fs)
			{
				IList<string> names = fs.ListXAttrs(path);
				return XAttrNamesToJSON(names);
			}
		}

		/// <summary>
		/// Executor that performs getting xattrs FileSystemAccess files system
		/// operation.
		/// </summary>
		public class FSGetXAttrs : FileSystemAccess.FileSystemExecutor<IDictionary>
		{
			private Path path;

			private IList<string> names;

			private XAttrCodec encoding;

			/// <summary>Creates getting xattrs executor.</summary>
			/// <param name="path">the path to retrieve the xattrs.</param>
			public FSGetXAttrs(string path, IList<string> names, XAttrCodec encoding)
			{
				this.path = new Path(path);
				this.names = names;
				this.encoding = encoding;
			}

			/// <summary>Executes the filesystem operation.</summary>
			/// <param name="fs">filesystem instance to use.</param>
			/// <returns>Map a map object (JSON friendly) with the xattrs.</returns>
			/// <exception cref="System.IO.IOException">thrown if an IO error occured.</exception>
			public virtual IDictionary Execute(FileSystem fs)
			{
				IDictionary<string, byte[]> xattrs = null;
				if (names != null && !names.IsEmpty())
				{
					xattrs = fs.GetXAttrs(path, names);
				}
				else
				{
					xattrs = fs.GetXAttrs(path);
				}
				return XAttrsToJSON(xattrs, encoding);
			}
		}
	}
}
