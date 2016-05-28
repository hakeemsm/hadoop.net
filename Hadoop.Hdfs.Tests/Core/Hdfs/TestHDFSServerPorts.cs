using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This test checks correctness of port usage by hdfs components:
	/// NameNode, DataNode, SecondaryNamenode and BackupNode.
	/// </summary>
	/// <remarks>
	/// This test checks correctness of port usage by hdfs components:
	/// NameNode, DataNode, SecondaryNamenode and BackupNode.
	/// The correct behavior is:<br />
	/// - when a specific port is provided the server must either start on that port
	/// or fail by throwing
	/// <see cref="Sharpen.BindException"/>
	/// .<br />
	/// - if the port = 0 (ephemeral) then the server should choose
	/// a free port and start on it.
	/// </remarks>
	public class TestHDFSServerPorts
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestHDFSServerPorts));

		internal static readonly string ThisHost = GetFullHostName() + ":0";

		private static readonly FilePath TestDataDir = PathUtils.GetTestDir(typeof(TestHDFSServerPorts
			));

		static TestHDFSServerPorts()
		{
			// reset default 0.0.0.0 addresses in order to avoid IPv6 problem
			DefaultMetricsSystem.SetMiniClusterMode(true);
		}

		internal Configuration config;

		internal FilePath hdfsDir;

		/// <summary>
		/// Attempt to determine the fully qualified domain name for this host
		/// to compare during testing.
		/// </summary>
		/// <remarks>
		/// Attempt to determine the fully qualified domain name for this host
		/// to compare during testing.
		/// This is necessary because in order for the BackupNode test to correctly
		/// work, the namenode must have its http server started with the fully
		/// qualified address, as this is the one the backupnode will attempt to start
		/// on as well.
		/// </remarks>
		/// <returns>Fully qualified hostname, or 127.0.0.1 if can't determine</returns>
		public static string GetFullHostName()
		{
			try
			{
				return DNS.GetDefaultHost("default");
			}
			catch (UnknownHostException)
			{
				Log.Warn("Unable to determine hostname.  May interfere with obtaining " + "valid test results."
					);
				return "127.0.0.1";
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual NameNode StartNameNode()
		{
			return StartNameNode(false);
		}

		/// <summary>Start the namenode.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual NameNode StartNameNode(bool withService)
		{
			hdfsDir = new FilePath(TestDataDir, "dfs");
			if (hdfsDir.Exists() && !FileUtil.FullyDelete(hdfsDir))
			{
				throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
			}
			config = new HdfsConfiguration();
			config.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Util.FileAsURI(new FilePath(hdfsDir
				, "name1")).ToString());
			FileSystem.SetDefaultUri(config, "hdfs://" + ThisHost);
			if (withService)
			{
				NameNode.SetServiceAddress(config, ThisHost);
			}
			config.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, ThisHost);
			DFSTestUtil.FormatNameNode(config);
			string[] args = new string[] {  };
			// NameNode will modify config with the ports it bound to
			return NameNode.CreateNameNode(args, config);
		}

		/// <summary>Start the BackupNode</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual BackupNode StartBackupNode(Configuration conf)
		{
			// Set up testing environment directories
			hdfsDir = new FilePath(TestDataDir, "backupNode");
			if (hdfsDir.Exists() && !FileUtil.FullyDelete(hdfsDir))
			{
				throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
			}
			FilePath currDir = new FilePath(hdfsDir, "name2");
			FilePath currDir2 = new FilePath(currDir, "current");
			FilePath currDir3 = new FilePath(currDir, "image");
			NUnit.Framework.Assert.IsTrue(currDir.Mkdirs());
			NUnit.Framework.Assert.IsTrue(currDir2.Mkdirs());
			NUnit.Framework.Assert.IsTrue(currDir3.Mkdirs());
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Util.FileAsURI(new FilePath(hdfsDir
				, "name2")).ToString());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, "${" + DFSConfigKeys.DfsNamenodeNameDirKey
				 + "}");
			// Start BackupNode
			string[] args = new string[] { HdfsServerConstants.StartupOption.Backup.GetName()
				 };
			BackupNode bu = (BackupNode)NameNode.CreateNameNode(args, conf);
			return bu;
		}

		/// <summary>Start the datanode.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual DataNode StartDataNode(int index, Configuration config)
		{
			FilePath dataNodeDir = new FilePath(TestDataDir, "data-" + index);
			config.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dataNodeDir.GetPath());
			string[] args = new string[] {  };
			// NameNode will modify config with the ports it bound to
			return DataNode.CreateDataNode(args, config);
		}

		/// <summary>Stop the datanode.</summary>
		public virtual void StopDataNode(DataNode dn)
		{
			if (dn != null)
			{
				dn.Shutdown();
			}
		}

		public virtual void StopNameNode(NameNode nn)
		{
			if (nn != null)
			{
				nn.Stop();
			}
		}

		public virtual Configuration GetConfig()
		{
			return this.config;
		}

		/// <summary>Check whether the namenode can be started.</summary>
		/// <exception cref="System.IO.IOException"/>
		private bool CanStartNameNode(Configuration conf)
		{
			NameNode nn2 = null;
			try
			{
				nn2 = NameNode.CreateNameNode(new string[] {  }, conf);
			}
			catch (IOException e)
			{
				if (e is BindException)
				{
					return false;
				}
				throw;
			}
			finally
			{
				StopNameNode(nn2);
			}
			return true;
		}

		/// <summary>Check whether the datanode can be started.</summary>
		/// <exception cref="System.IO.IOException"/>
		private bool CanStartDataNode(Configuration conf)
		{
			DataNode dn = null;
			try
			{
				dn = DataNode.CreateDataNode(new string[] {  }, conf);
			}
			catch (IOException e)
			{
				if (e is BindException)
				{
					return false;
				}
				throw;
			}
			finally
			{
				if (dn != null)
				{
					dn.Shutdown();
				}
			}
			return true;
		}

		/// <summary>Check whether the secondary name-node can be started.</summary>
		/// <exception cref="System.IO.IOException"/>
		private bool CanStartSecondaryNode(Configuration conf)
		{
			// Using full name allows us not to have to add deprecation tag to
			// entire source file.
			SecondaryNameNode sn = null;
			try
			{
				sn = new SecondaryNameNode(conf);
			}
			catch (IOException e)
			{
				if (e is BindException)
				{
					return false;
				}
				throw;
			}
			finally
			{
				if (sn != null)
				{
					sn.Shutdown();
				}
			}
			return true;
		}

		/// <summary>Check whether the BackupNode can be started.</summary>
		/// <exception cref="System.IO.IOException"/>
		private bool CanStartBackupNode(Configuration conf)
		{
			BackupNode bn = null;
			try
			{
				bn = StartBackupNode(conf);
			}
			catch (IOException e)
			{
				if (e is BindException)
				{
					return false;
				}
				throw;
			}
			finally
			{
				if (bn != null)
				{
					bn.Stop();
				}
			}
			return true;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNameNodePorts()
		{
			RunTestNameNodePorts(false);
			RunTestNameNodePorts(true);
		}

		/// <summary>Verify namenode port usage.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void RunTestNameNodePorts(bool withService)
		{
			NameNode nn = null;
			try
			{
				nn = StartNameNode(withService);
				// start another namenode on the same port
				Configuration conf2 = new HdfsConfiguration(config);
				conf2.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Util.FileAsURI(new FilePath(hdfsDir
					, "name2")).ToString());
				DFSTestUtil.FormatNameNode(conf2);
				bool started = CanStartNameNode(conf2);
				NUnit.Framework.Assert.IsFalse(started);
				// should fail
				// start on a different main port
				FileSystem.SetDefaultUri(conf2, "hdfs://" + ThisHost);
				started = CanStartNameNode(conf2);
				NUnit.Framework.Assert.IsFalse(started);
				// should fail again
				// reset conf2 since NameNode modifies it
				FileSystem.SetDefaultUri(conf2, "hdfs://" + ThisHost);
				// different http port
				conf2.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, ThisHost);
				started = CanStartNameNode(conf2);
				if (withService)
				{
					NUnit.Framework.Assert.IsFalse("Should've failed on service port", started);
					// reset conf2 since NameNode modifies it
					FileSystem.SetDefaultUri(conf2, "hdfs://" + ThisHost);
					conf2.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, ThisHost);
					// Set Service address      
					conf2.Set(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, ThisHost);
					started = CanStartNameNode(conf2);
				}
				NUnit.Framework.Assert.IsTrue(started);
			}
			finally
			{
				StopNameNode(nn);
			}
		}

		/// <summary>Verify datanode port usage.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDataNodePorts()
		{
			NameNode nn = null;
			try
			{
				nn = StartNameNode();
				// start data-node on the same port as name-node
				Configuration conf2 = new HdfsConfiguration(config);
				conf2.Set(DFSConfigKeys.DfsDatanodeDataDirKey, new FilePath(hdfsDir, "data").GetPath
					());
				conf2.Set(DFSConfigKeys.DfsDatanodeAddressKey, FileSystem.GetDefaultUri(config).GetAuthority
					());
				conf2.Set(DFSConfigKeys.DfsDatanodeHttpAddressKey, ThisHost);
				bool started = CanStartDataNode(conf2);
				NUnit.Framework.Assert.IsFalse(started);
				// should fail
				// bind http server to the same port as name-node
				conf2.Set(DFSConfigKeys.DfsDatanodeAddressKey, ThisHost);
				conf2.Set(DFSConfigKeys.DfsDatanodeHttpAddressKey, config.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey
					));
				started = CanStartDataNode(conf2);
				NUnit.Framework.Assert.IsFalse(started);
				// should fail
				// both ports are different from the name-node ones
				conf2.Set(DFSConfigKeys.DfsDatanodeAddressKey, ThisHost);
				conf2.Set(DFSConfigKeys.DfsDatanodeHttpAddressKey, ThisHost);
				conf2.Set(DFSConfigKeys.DfsDatanodeIpcAddressKey, ThisHost);
				started = CanStartDataNode(conf2);
				NUnit.Framework.Assert.IsTrue(started);
			}
			finally
			{
				// should start now
				StopNameNode(nn);
			}
		}

		/// <summary>Verify secondary namenode port usage.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSecondaryNodePorts()
		{
			NameNode nn = null;
			try
			{
				nn = StartNameNode();
				// bind http server to the same port as name-node
				Configuration conf2 = new HdfsConfiguration(config);
				conf2.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, config.Get(DFSConfigKeys
					.DfsNamenodeHttpAddressKey));
				Log.Info("= Starting 1 on: " + conf2.Get(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey
					));
				bool started = CanStartSecondaryNode(conf2);
				NUnit.Framework.Assert.IsFalse(started);
				// should fail
				// bind http server to a different port
				conf2.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, ThisHost);
				Log.Info("= Starting 2 on: " + conf2.Get(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey
					));
				started = CanStartSecondaryNode(conf2);
				NUnit.Framework.Assert.IsTrue(started);
			}
			finally
			{
				// should start now
				StopNameNode(nn);
			}
		}

		/// <summary>Verify BackupNode port usage.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBackupNodePorts()
		{
			NameNode nn = null;
			try
			{
				nn = StartNameNode();
				Configuration backup_config = new HdfsConfiguration(config);
				backup_config.Set(DFSConfigKeys.DfsNamenodeBackupAddressKey, ThisHost);
				// bind http server to the same port as name-node
				backup_config.Set(DFSConfigKeys.DfsNamenodeBackupHttpAddressKey, backup_config.Get
					(DFSConfigKeys.DfsNamenodeHttpAddressKey));
				Log.Info("= Starting 1 on: " + backup_config.Get(DFSConfigKeys.DfsNamenodeBackupHttpAddressKey
					));
				NUnit.Framework.Assert.IsFalse("Backup started on same port as Namenode", CanStartBackupNode
					(backup_config));
				// should fail
				// bind http server to a different port
				backup_config.Set(DFSConfigKeys.DfsNamenodeBackupHttpAddressKey, ThisHost);
				Log.Info("= Starting 2 on: " + backup_config.Get(DFSConfigKeys.DfsNamenodeBackupHttpAddressKey
					));
				bool started = CanStartBackupNode(backup_config);
				NUnit.Framework.Assert.IsTrue("Backup Namenode should've started", started);
			}
			finally
			{
				// should start now
				StopNameNode(nn);
			}
		}
	}
}
