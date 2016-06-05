using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>The class provides utilities for accessing a NameNode.</summary>
	public class NameNodeConnector : IDisposable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector
			));

		public const int DefaultMaxIdleIterations = 5;

		private static bool write2IdFile = true;

		/// <summary>
		/// Create
		/// <see cref="NameNodeConnector"/>
		/// for the given namenodes.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static IList<Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector> NewNameNodeConnectors
			(ICollection<URI> namenodes, string name, Path idPath, Configuration conf, int maxIdleIterations
			)
		{
			IList<Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector> connectors = new 
				AList<Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector>(namenodes.Count);
			foreach (URI uri in namenodes)
			{
				Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector nnc = new Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector
					(name, uri, idPath, null, conf, maxIdleIterations);
				nnc.GetKeyManager().StartBlockKeyUpdater();
				connectors.AddItem(nnc);
			}
			return connectors;
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector> NewNameNodeConnectors
			(IDictionary<URI, IList<Path>> namenodes, string name, Path idPath, Configuration
			 conf, int maxIdleIterations)
		{
			IList<Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector> connectors = new 
				AList<Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector>(namenodes.Count);
			foreach (KeyValuePair<URI, IList<Path>> entry in namenodes)
			{
				Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector nnc = new Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector
					(name, entry.Key, idPath, entry.Value, conf, maxIdleIterations);
				nnc.GetKeyManager().StartBlockKeyUpdater();
				connectors.AddItem(nnc);
			}
			return connectors;
		}

		[VisibleForTesting]
		public static void SetWrite2IdFile(bool write2IdFile)
		{
			Org.Apache.Hadoop.Hdfs.Server.Balancer.NameNodeConnector.write2IdFile = write2IdFile;
		}

		private readonly URI nameNodeUri;

		private readonly string blockpoolID;

		private readonly NamenodeProtocol namenode;

		private readonly ClientProtocol client;

		private readonly KeyManager keyManager;

		internal readonly AtomicBoolean fallbackToSimpleAuth = new AtomicBoolean(false);

		private readonly DistributedFileSystem fs;

		private readonly Path idPath;

		private readonly OutputStream @out;

		private readonly IList<Path> targetPaths;

		private readonly AtomicLong bytesMoved = new AtomicLong();

		private readonly int maxNotChangedIterations;

		private int notChangedIterations = 0;

		/// <exception cref="System.IO.IOException"/>
		public NameNodeConnector(string name, URI nameNodeUri, Path idPath, IList<Path> targetPaths
			, Configuration conf, int maxNotChangedIterations)
		{
			this.nameNodeUri = nameNodeUri;
			this.idPath = idPath;
			this.targetPaths = targetPaths == null || targetPaths.IsEmpty() ? Arrays.AsList(new 
				Path("/")) : targetPaths;
			this.maxNotChangedIterations = maxNotChangedIterations;
			this.namenode = NameNodeProxies.CreateProxy<NamenodeProtocol>(conf, nameNodeUri).
				GetProxy();
			this.client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, nameNodeUri, fallbackToSimpleAuth
				).GetProxy();
			this.fs = (DistributedFileSystem)FileSystem.Get(nameNodeUri, conf);
			NamespaceInfo namespaceinfo = namenode.VersionRequest();
			this.blockpoolID = namespaceinfo.GetBlockPoolID();
			FsServerDefaults defaults = fs.GetServerDefaults(new Path("/"));
			this.keyManager = new KeyManager(blockpoolID, namenode, defaults.GetEncryptDataTransfer
				(), conf);
			// if it is for test, we do not create the id file
			@out = CheckAndMarkRunning();
			if (@out == null)
			{
				// Exit if there is another one running.
				throw new IOException("Another " + name + " is running.");
			}
		}

		public virtual DistributedFileSystem GetDistributedFileSystem()
		{
			return fs;
		}

		/// <returns>the block pool ID</returns>
		public virtual string GetBlockpoolID()
		{
			return blockpoolID;
		}

		internal virtual AtomicLong GetBytesMoved()
		{
			return bytesMoved;
		}

		/// <returns>blocks with locations.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual BlocksWithLocations GetBlocks(DatanodeInfo datanode, long size)
		{
			return namenode.GetBlocks(datanode, size);
		}

		/// <returns>live datanode storage reports.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeStorageReport[] GetLiveDatanodeStorageReport()
		{
			return client.GetDatanodeStorageReport(HdfsConstants.DatanodeReportType.Live);
		}

		/// <returns>the key manager</returns>
		public virtual KeyManager GetKeyManager()
		{
			return keyManager;
		}

		/// <returns>the list of paths to scan/migrate</returns>
		public virtual IList<Path> GetTargetPaths()
		{
			return targetPaths;
		}

		/// <summary>Should the instance continue running?</summary>
		public virtual bool ShouldContinue(long dispatchBlockMoveBytes)
		{
			if (dispatchBlockMoveBytes > 0)
			{
				notChangedIterations = 0;
			}
			else
			{
				notChangedIterations++;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("No block has been moved for " + notChangedIterations + " iterations, "
						 + "maximum notChangedIterations before exit is: " + ((maxNotChangedIterations >=
						 0) ? maxNotChangedIterations : "Infinite"));
				}
				if ((maxNotChangedIterations >= 0) && (notChangedIterations >= maxNotChangedIterations
					))
				{
					System.Console.Out.WriteLine("No block has been moved for " + notChangedIterations
						 + " iterations. Exiting...");
					return false;
				}
			}
			return true;
		}

		/// <summary>
		/// The idea for making sure that there is no more than one instance
		/// running in an HDFS is to create a file in the HDFS, writes the hostname
		/// of the machine on which the instance is running to the file, but did not
		/// close the file until it exits.
		/// </summary>
		/// <remarks>
		/// The idea for making sure that there is no more than one instance
		/// running in an HDFS is to create a file in the HDFS, writes the hostname
		/// of the machine on which the instance is running to the file, but did not
		/// close the file until it exits.
		/// This prevents the second instance from running because it can not
		/// creates the file while the first one is running.
		/// This method checks if there is any running instance. If no, mark yes.
		/// Note that this is an atomic operation.
		/// </remarks>
		/// <returns>
		/// null if there is a running instance;
		/// otherwise, the output stream to the newly created file.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private OutputStream CheckAndMarkRunning()
		{
			try
			{
				if (fs.Exists(idPath))
				{
					// try appending to it so that it will fail fast if another balancer is
					// running.
					IOUtils.CloseStream(fs.Append(idPath));
					fs.Delete(idPath, true);
				}
				FSDataOutputStream fsout = fs.Create(idPath, false);
				// mark balancer idPath to be deleted during filesystem closure
				fs.DeleteOnExit(idPath);
				if (write2IdFile)
				{
					fsout.WriteBytes(Sharpen.Runtime.GetLocalHost().GetHostName());
					fsout.Hflush();
				}
				return fsout;
			}
			catch (RemoteException e)
			{
				if (typeof(AlreadyBeingCreatedException).FullName.Equals(e.GetClassName()))
				{
					return null;
				}
				else
				{
					throw;
				}
			}
		}

		public virtual void Close()
		{
			keyManager.Close();
			// close the output file
			IOUtils.CloseStream(@out);
			if (fs != null)
			{
				try
				{
					fs.Delete(idPath, true);
				}
				catch (IOException ioe)
				{
					Log.Warn("Failed to delete " + idPath, ioe);
				}
			}
		}

		public override string ToString()
		{
			return GetType().Name + "[namenodeUri=" + nameNodeUri + ", bpid=" + blockpoolID +
				 "]";
		}
	}
}
