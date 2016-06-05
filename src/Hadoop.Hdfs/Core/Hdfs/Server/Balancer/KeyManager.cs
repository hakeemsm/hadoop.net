using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>The class provides utilities for key and token management.</summary>
	public class KeyManager : IDisposable, DataEncryptionKeyFactory
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Balancer.KeyManager
			));

		private readonly NamenodeProtocol namenode;

		private readonly bool isBlockTokenEnabled;

		private readonly bool encryptDataTransfer;

		private bool shouldRun;

		private readonly BlockTokenSecretManager blockTokenSecretManager;

		private readonly KeyManager.BlockKeyUpdater blockKeyUpdater;

		private DataEncryptionKey encryptionKey;

		/// <exception cref="System.IO.IOException"/>
		public KeyManager(string blockpoolID, NamenodeProtocol namenode, bool encryptDataTransfer
			, Configuration conf)
		{
			this.namenode = namenode;
			this.encryptDataTransfer = encryptDataTransfer;
			ExportedBlockKeys keys = namenode.GetBlockKeys();
			this.isBlockTokenEnabled = keys.IsBlockTokenEnabled();
			if (isBlockTokenEnabled)
			{
				long updateInterval = keys.GetKeyUpdateInterval();
				long tokenLifetime = keys.GetTokenLifetime();
				Log.Info("Block token params received from NN: update interval=" + StringUtils.FormatTime
					(updateInterval) + ", token lifetime=" + StringUtils.FormatTime(tokenLifetime));
				string encryptionAlgorithm = conf.Get(DFSConfigKeys.DfsDataEncryptionAlgorithmKey
					);
				this.blockTokenSecretManager = new BlockTokenSecretManager(updateInterval, tokenLifetime
					, blockpoolID, encryptionAlgorithm);
				this.blockTokenSecretManager.AddKeys(keys);
				// sync block keys with NN more frequently than NN updates its block keys
				this.blockKeyUpdater = new KeyManager.BlockKeyUpdater(this, updateInterval / 4);
				this.shouldRun = true;
			}
			else
			{
				this.blockTokenSecretManager = null;
				this.blockKeyUpdater = null;
			}
		}

		public virtual void StartBlockKeyUpdater()
		{
			if (blockKeyUpdater != null)
			{
				blockKeyUpdater.daemon.Start();
			}
		}

		/// <summary>Get an access token for a block.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> GetAccessToken
			(ExtendedBlock eb)
		{
			if (!isBlockTokenEnabled)
			{
				return BlockTokenSecretManager.DummyToken;
			}
			else
			{
				if (!shouldRun)
				{
					throw new IOException("Cannot get access token since BlockKeyUpdater is not running"
						);
				}
				return blockTokenSecretManager.GenerateToken(null, eb, EnumSet.Of(BlockTokenSecretManager.AccessMode
					.Replace, BlockTokenSecretManager.AccessMode.Copy));
			}
		}

		public virtual DataEncryptionKey NewDataEncryptionKey()
		{
			if (encryptDataTransfer)
			{
				lock (this)
				{
					if (encryptionKey == null)
					{
						encryptionKey = blockTokenSecretManager.GenerateDataEncryptionKey();
					}
					return encryptionKey;
				}
			}
			else
			{
				return null;
			}
		}

		public virtual void Close()
		{
			shouldRun = false;
			try
			{
				if (blockKeyUpdater != null)
				{
					blockKeyUpdater.daemon.Interrupt();
				}
			}
			catch (Exception e)
			{
				Log.Warn("Exception shutting down access key updater thread", e);
			}
		}

		/// <summary>Periodically updates access keys.</summary>
		internal class BlockKeyUpdater : Runnable, IDisposable
		{
			private readonly Daemon daemon;

			private readonly long sleepInterval;

			internal BlockKeyUpdater(KeyManager _enclosing, long sleepInterval)
			{
				this._enclosing = _enclosing;
				daemon = new Daemon(this);
				this.sleepInterval = sleepInterval;
				KeyManager.Log.Info("Update block keys every " + StringUtils.FormatTime(sleepInterval
					));
			}

			public virtual void Run()
			{
				try
				{
					while (this._enclosing.shouldRun)
					{
						try
						{
							this._enclosing.blockTokenSecretManager.AddKeys(this._enclosing.namenode.GetBlockKeys
								());
						}
						catch (IOException e)
						{
							KeyManager.Log.Error("Failed to set keys", e);
						}
						Sharpen.Thread.Sleep(this.sleepInterval);
					}
				}
				catch (Exception e)
				{
					KeyManager.Log.Debug("InterruptedException in block key updater thread", e);
				}
				catch (Exception e)
				{
					KeyManager.Log.Error("Exception in block key updater thread", e);
					this._enclosing.shouldRun = false;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				try
				{
					this.daemon.Interrupt();
				}
				catch (Exception e)
				{
					KeyManager.Log.Warn("Exception shutting down key updater thread", e);
				}
			}

			private readonly KeyManager _enclosing;
		}
	}
}
