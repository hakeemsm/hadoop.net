using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// NameNodeResourceChecker provides a method -
	/// <code>hasAvailableDiskSpace</code> - which will return true if and only if
	/// the NameNode has disk space available on all required volumes, and any volume
	/// which is configured to be redundant.
	/// </summary>
	/// <remarks>
	/// NameNodeResourceChecker provides a method -
	/// <code>hasAvailableDiskSpace</code> - which will return true if and only if
	/// the NameNode has disk space available on all required volumes, and any volume
	/// which is configured to be redundant. Volumes containing file system edits dirs
	/// are added by default, and arbitrary extra volumes may be configured as well.
	/// </remarks>
	public class NameNodeResourceChecker
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNodeResourceChecker
			).FullName);

		private readonly long duReserved;

		private readonly Configuration conf;

		private IDictionary<string, NameNodeResourceChecker.CheckedVolume> volumes;

		private int minimumRedundantVolumes;

		internal class CheckedVolume : CheckableNameNodeResource
		{
			private DF df;

			private bool required;

			private string volume;

			/// <exception cref="System.IO.IOException"/>
			public CheckedVolume(NameNodeResourceChecker _enclosing, FilePath dirToCheck, bool
				 required)
			{
				this._enclosing = _enclosing;
				// Space (in bytes) reserved per volume.
				this.df = new DF(dirToCheck, this._enclosing.conf);
				this.required = required;
				this.volume = this.df.GetFilesystem();
			}

			public virtual string GetVolume()
			{
				return this.volume;
			}

			public virtual bool IsRequired()
			{
				return this.required;
			}

			public virtual bool IsResourceAvailable()
			{
				long availableSpace = this.df.GetAvailable();
				if (NameNodeResourceChecker.Log.IsDebugEnabled())
				{
					NameNodeResourceChecker.Log.Debug("Space available on volume '" + this.volume + "' is "
						 + availableSpace);
				}
				if (availableSpace < this._enclosing.duReserved)
				{
					NameNodeResourceChecker.Log.Warn("Space available on volume '" + this.volume + "' is "
						 + availableSpace + ", which is below the configured reserved amount " + this._enclosing
						.duReserved);
					return false;
				}
				else
				{
					return true;
				}
			}

			public override string ToString()
			{
				return "volume: " + this.volume + " required: " + this.required + " resource available: "
					 + this.IsResourceAvailable();
			}

			private readonly NameNodeResourceChecker _enclosing;
		}

		/// <summary>
		/// Create a NameNodeResourceChecker, which will check the edits dirs and any
		/// additional dirs to check set in <code>conf</code>.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public NameNodeResourceChecker(Configuration conf)
		{
			this.conf = conf;
			volumes = new Dictionary<string, NameNodeResourceChecker.CheckedVolume>();
			duReserved = conf.GetLong(DFSConfigKeys.DfsNamenodeDuReservedKey, DFSConfigKeys.DfsNamenodeDuReservedDefault
				);
			ICollection<URI> extraCheckedVolumes = Util.StringCollectionAsURIs(conf.GetTrimmedStringCollection
				(DFSConfigKeys.DfsNamenodeCheckedVolumesKey));
			ICollection<URI> localEditDirs = Collections2.Filter(FSNamesystem.GetNamespaceEditsDirs
				(conf), new _Predicate_121());
			// Add all the local edits dirs, marking some as required if they are
			// configured as such.
			foreach (URI editsDirToCheck in localEditDirs)
			{
				AddDirToCheck(editsDirToCheck, FSNamesystem.GetRequiredNamespaceEditsDirs(conf).Contains
					(editsDirToCheck));
			}
			// All extra checked volumes are marked "required"
			foreach (URI extraDirToCheck in extraCheckedVolumes)
			{
				AddDirToCheck(extraDirToCheck, true);
			}
			minimumRedundantVolumes = conf.GetInt(DFSConfigKeys.DfsNamenodeCheckedVolumesMinimumKey
				, DFSConfigKeys.DfsNamenodeCheckedVolumesMinimumDefault);
		}

		private sealed class _Predicate_121 : Predicate<URI>
		{
			public _Predicate_121()
			{
			}

			public bool Apply(URI input)
			{
				if (input.GetScheme().Equals(NNStorage.LocalUriScheme))
				{
					return true;
				}
				return false;
			}
		}

		/// <summary>Add the volume of the passed-in directory to the list of volumes to check.
		/// 	</summary>
		/// <remarks>
		/// Add the volume of the passed-in directory to the list of volumes to check.
		/// If <code>required</code> is true, and this volume is already present, but
		/// is marked redundant, it will be marked required. If the volume is already
		/// present but marked required then this method is a no-op.
		/// </remarks>
		/// <param name="directoryToCheck">The directory whose volume will be checked for available space.
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		private void AddDirToCheck(URI directoryToCheck, bool required)
		{
			FilePath dir = new FilePath(directoryToCheck.GetPath());
			if (!dir.Exists())
			{
				throw new IOException("Missing directory " + dir.GetAbsolutePath());
			}
			NameNodeResourceChecker.CheckedVolume newVolume = new NameNodeResourceChecker.CheckedVolume
				(this, dir, required);
			NameNodeResourceChecker.CheckedVolume volume = volumes[newVolume.GetVolume()];
			if (volume == null || !volume.IsRequired())
			{
				volumes[newVolume.GetVolume()] = newVolume;
			}
		}

		/// <summary>
		/// Return true if disk space is available on at least one of the configured
		/// redundant volumes, and all of the configured required volumes.
		/// </summary>
		/// <returns>
		/// True if the configured amount of disk space is available on at
		/// least one redundant volume and all of the required volumes, false
		/// otherwise.
		/// </returns>
		public virtual bool HasAvailableDiskSpace()
		{
			return NameNodeResourcePolicy.AreResourcesAvailable(volumes.Values, minimumRedundantVolumes
				);
		}

		/// <summary>Return the set of directories which are low on space.</summary>
		/// <returns>the set of directories whose free space is below the threshold.</returns>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual ICollection<string> GetVolumesLowOnSpace()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Going to check the following volumes disk space: " + volumes);
			}
			ICollection<string> lowVolumes = new AList<string>();
			foreach (NameNodeResourceChecker.CheckedVolume volume in volumes.Values)
			{
				lowVolumes.AddItem(volume.GetVolume());
			}
			return lowVolumes;
		}

		[VisibleForTesting]
		internal virtual void SetVolumes(IDictionary<string, NameNodeResourceChecker.CheckedVolume
			> volumes)
		{
			this.volumes = volumes;
		}

		[VisibleForTesting]
		internal virtual void SetMinimumReduntdantVolumes(int minimumRedundantVolumes)
		{
			this.minimumRedundantVolumes = minimumRedundantVolumes;
		}
	}
}
