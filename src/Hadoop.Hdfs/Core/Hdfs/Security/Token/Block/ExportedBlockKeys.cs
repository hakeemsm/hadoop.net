using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Block
{
	/// <summary>Object for passing block keys</summary>
	public class ExportedBlockKeys : Writable
	{
		public static readonly Org.Apache.Hadoop.Hdfs.Security.Token.Block.ExportedBlockKeys
			 DummyKeys = new Org.Apache.Hadoop.Hdfs.Security.Token.Block.ExportedBlockKeys();

		private bool isBlockTokenEnabled;

		private long keyUpdateInterval;

		private long tokenLifetime;

		private readonly BlockKey currentKey;

		private BlockKey[] allKeys;

		public ExportedBlockKeys()
			: this(false, 0, 0, new BlockKey(), new BlockKey[0])
		{
		}

		public ExportedBlockKeys(bool isBlockTokenEnabled, long keyUpdateInterval, long tokenLifetime
			, BlockKey currentKey, BlockKey[] allKeys)
		{
			this.isBlockTokenEnabled = isBlockTokenEnabled;
			this.keyUpdateInterval = keyUpdateInterval;
			this.tokenLifetime = tokenLifetime;
			this.currentKey = currentKey == null ? new BlockKey() : currentKey;
			this.allKeys = allKeys == null ? new BlockKey[0] : allKeys;
		}

		public virtual bool IsBlockTokenEnabled()
		{
			return isBlockTokenEnabled;
		}

		public virtual long GetKeyUpdateInterval()
		{
			return keyUpdateInterval;
		}

		public virtual long GetTokenLifetime()
		{
			return tokenLifetime;
		}

		public virtual BlockKey GetCurrentKey()
		{
			return currentKey;
		}

		public virtual BlockKey[] GetAllKeys()
		{
			return allKeys;
		}

		static ExportedBlockKeys()
		{
			// ///////////////////////////////////////////////
			// Writable
			// ///////////////////////////////////////////////
			// register a ctor
			WritableFactories.SetFactory(typeof(Org.Apache.Hadoop.Hdfs.Security.Token.Block.ExportedBlockKeys
				), new _WritableFactory_80());
		}

		private sealed class _WritableFactory_80 : WritableFactory
		{
			public _WritableFactory_80()
			{
			}

			public Writable NewInstance()
			{
				return new Org.Apache.Hadoop.Hdfs.Security.Token.Block.ExportedBlockKeys();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteBoolean(isBlockTokenEnabled);
			@out.WriteLong(keyUpdateInterval);
			@out.WriteLong(tokenLifetime);
			currentKey.Write(@out);
			@out.WriteInt(allKeys.Length);
			for (int i = 0; i < allKeys.Length; i++)
			{
				allKeys[i].Write(@out);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			isBlockTokenEnabled = @in.ReadBoolean();
			keyUpdateInterval = @in.ReadLong();
			tokenLifetime = @in.ReadLong();
			currentKey.ReadFields(@in);
			this.allKeys = new BlockKey[@in.ReadInt()];
			for (int i = 0; i < allKeys.Length; i++)
			{
				allKeys[i] = new BlockKey();
				allKeys[i].ReadFields(@in);
			}
		}
	}
}
