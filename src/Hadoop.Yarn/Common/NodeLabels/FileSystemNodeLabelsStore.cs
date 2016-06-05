using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels
{
	public class FileSystemNodeLabelsStore : NodeLabelsStore
	{
		public FileSystemNodeLabelsStore(CommonNodeLabelsManager mgr)
			: base(mgr)
		{
		}

		protected internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Nodelabels.FileSystemNodeLabelsStore
			));

		protected internal const string DefaultDirName = "node-labels";

		protected internal const string MirrorFilename = "nodelabel.mirror";

		protected internal const string EditlogFilename = "nodelabel.editlog";

		protected internal enum SerializedLogType
		{
			AddLabels,
			NodeToLabels,
			RemoveLabels
		}

		internal Path fsWorkingPath;

		internal FileSystem fs;

		internal FSDataOutputStream editlogOs;

		internal Path editLogPath;

		/// <exception cref="System.IO.IOException"/>
		private string GetDefaultFSNodeLabelsRootDir()
		{
			// default is in local: /tmp/hadoop-yarn-${user}/node-labels/
			return "file:///tmp/hadoop-yarn-" + UserGroupInformation.GetCurrentUser().GetShortUserName
				() + "/" + DefaultDirName;
		}

		/// <exception cref="System.Exception"/>
		public override void Init(Configuration conf)
		{
			fsWorkingPath = new Path(conf.Get(YarnConfiguration.FsNodeLabelsStoreRootDir, GetDefaultFSNodeLabelsRootDir
				()));
			SetFileSystem(conf);
			// mkdir of root dir path
			if (!fs.Exists(fsWorkingPath))
			{
				fs.Mkdirs(fsWorkingPath);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			try
			{
				fs.Close();
				editlogOs.Close();
			}
			catch (IOException e)
			{
				Log.Warn("Exception happened whiling shutting down,", e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetFileSystem(Configuration conf)
		{
			Configuration confCopy = new Configuration(conf);
			confCopy.SetBoolean("dfs.client.retry.policy.enabled", true);
			string retryPolicy = confCopy.Get(YarnConfiguration.FsNodeLabelsStoreRetryPolicySpec
				, YarnConfiguration.DefaultFsNodeLabelsStoreRetryPolicySpec);
			confCopy.Set("dfs.client.retry.policy.spec", retryPolicy);
			fs = fsWorkingPath.GetFileSystem(confCopy);
			// if it's local file system, use RawLocalFileSystem instead of
			// LocalFileSystem, the latter one doesn't support append.
			if (fs.GetScheme().Equals("file"))
			{
				fs = ((LocalFileSystem)fs).GetRaw();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void EnsureAppendEditlogFile()
		{
			editlogOs = fs.Append(editLogPath);
		}

		/// <exception cref="System.IO.IOException"/>
		private void EnsureCloseEditlogFile()
		{
			editlogOs.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void UpdateNodeToLabelsMappings(IDictionary<NodeId, ICollection<string
			>> nodeToLabels)
		{
			EnsureAppendEditlogFile();
			editlogOs.WriteInt((int)(FileSystemNodeLabelsStore.SerializedLogType.NodeToLabels
				));
			((ReplaceLabelsOnNodeRequestPBImpl)ReplaceLabelsOnNodeRequest.NewInstance(nodeToLabels
				)).GetProto().WriteDelimitedTo(editlogOs);
			EnsureCloseEditlogFile();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreNewClusterNodeLabels(ICollection<string> labels)
		{
			EnsureAppendEditlogFile();
			editlogOs.WriteInt((int)(FileSystemNodeLabelsStore.SerializedLogType.AddLabels));
			((AddToClusterNodeLabelsRequestPBImpl)AddToClusterNodeLabelsRequest.NewInstance(labels
				)).GetProto().WriteDelimitedTo(editlogOs);
			EnsureCloseEditlogFile();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveClusterNodeLabels(ICollection<string> labels)
		{
			EnsureAppendEditlogFile();
			editlogOs.WriteInt((int)(FileSystemNodeLabelsStore.SerializedLogType.RemoveLabels
				));
			((RemoveFromClusterNodeLabelsRequestPBImpl)RemoveFromClusterNodeLabelsRequest.NewInstance
				(Sets.NewHashSet(labels.GetEnumerator()))).GetProto().WriteDelimitedTo(editlogOs
				);
			EnsureCloseEditlogFile();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Recover()
		{
			/*
			* Steps of recover
			* 1) Read from last mirror (from mirror or mirror.old)
			* 2) Read from last edit log, and apply such edit log
			* 3) Write new mirror to mirror.writing
			* 4) Rename mirror to mirror.old
			* 5) Move mirror.writing to mirror
			* 6) Remove mirror.old
			* 7) Remove edit log and create a new empty edit log
			*/
			// Open mirror from serialized file
			Path mirrorPath = new Path(fsWorkingPath, MirrorFilename);
			Path oldMirrorPath = new Path(fsWorkingPath, MirrorFilename + ".old");
			FSDataInputStream @is = null;
			if (fs.Exists(mirrorPath))
			{
				@is = fs.Open(mirrorPath);
			}
			else
			{
				if (fs.Exists(oldMirrorPath))
				{
					@is = fs.Open(oldMirrorPath);
				}
			}
			if (null != @is)
			{
				ICollection<string> labels = new AddToClusterNodeLabelsRequestPBImpl(YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
					.ParseDelimitedFrom(@is)).GetNodeLabels();
				IDictionary<NodeId, ICollection<string>> nodeToLabels = new ReplaceLabelsOnNodeRequestPBImpl
					(YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto.ParseDelimitedFrom
					(@is)).GetNodeToLabels();
				mgr.AddToCluserNodeLabels(labels);
				mgr.ReplaceLabelsOnNode(nodeToLabels);
				@is.Close();
			}
			// Open and process editlog
			editLogPath = new Path(fsWorkingPath, EditlogFilename);
			if (fs.Exists(editLogPath))
			{
				@is = fs.Open(editLogPath);
				while (true)
				{
					try
					{
						// read edit log one by one
						FileSystemNodeLabelsStore.SerializedLogType type = FileSystemNodeLabelsStore.SerializedLogType
							.Values()[@is.ReadInt()];
						switch (type)
						{
							case FileSystemNodeLabelsStore.SerializedLogType.AddLabels:
							{
								ICollection<string> labels = YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
									.ParseDelimitedFrom(@is).GetNodeLabelsList();
								mgr.AddToCluserNodeLabels(Sets.NewHashSet(labels.GetEnumerator()));
								break;
							}

							case FileSystemNodeLabelsStore.SerializedLogType.RemoveLabels:
							{
								ICollection<string> labels = YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto
									.ParseDelimitedFrom(@is).GetNodeLabelsList();
								mgr.RemoveFromClusterNodeLabels(labels);
								break;
							}

							case FileSystemNodeLabelsStore.SerializedLogType.NodeToLabels:
							{
								IDictionary<NodeId, ICollection<string>> map = new ReplaceLabelsOnNodeRequestPBImpl
									(YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto.ParseDelimitedFrom
									(@is)).GetNodeToLabels();
								mgr.ReplaceLabelsOnNode(map);
								break;
							}
						}
					}
					catch (EOFException)
					{
						// EOF hit, break
						break;
					}
				}
			}
			// Serialize current mirror to mirror.writing
			Path writingMirrorPath = new Path(fsWorkingPath, MirrorFilename + ".writing");
			FSDataOutputStream os = fs.Create(writingMirrorPath, true);
			((AddToClusterNodeLabelsRequestPBImpl)AddToClusterNodeLabelsRequestPBImpl.NewInstance
				(mgr.GetClusterNodeLabels())).GetProto().WriteDelimitedTo(os);
			((ReplaceLabelsOnNodeRequestPBImpl)ReplaceLabelsOnNodeRequest.NewInstance(mgr.GetNodeLabels
				())).GetProto().WriteDelimitedTo(os);
			os.Close();
			// Move mirror to mirror.old
			if (fs.Exists(mirrorPath))
			{
				fs.Delete(oldMirrorPath, false);
				fs.Rename(mirrorPath, oldMirrorPath);
			}
			// move mirror.writing to mirror
			fs.Rename(writingMirrorPath, mirrorPath);
			fs.Delete(writingMirrorPath, false);
			// remove mirror.old
			fs.Delete(oldMirrorPath, false);
			// create a new editlog file
			editlogOs = fs.Create(editLogPath, true);
			editlogOs.Close();
			Log.Info("Finished write mirror at:" + mirrorPath.ToString());
			Log.Info("Finished create editlog file at:" + editLogPath.ToString());
		}
	}
}
