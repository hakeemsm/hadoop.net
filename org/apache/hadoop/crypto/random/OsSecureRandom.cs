using Sharpen;

namespace org.apache.hadoop.crypto.random
{
	/// <summary>
	/// A Random implementation that uses random bytes sourced from the
	/// operating system.
	/// </summary>
	[System.Serializable]
	public class OsSecureRandom : java.util.Random, java.io.Closeable, org.apache.hadoop.conf.Configurable
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.crypto.random.OsSecureRandom
			)));

		private const long serialVersionUID = 6391500337172057900L;

		[System.NonSerialized]
		private org.apache.hadoop.conf.Configuration conf;

		private readonly int RESERVOIR_LENGTH = 8192;

		private string randomDevPath;

		[System.NonSerialized]
		private java.io.FileInputStream stream;

		private readonly byte[] reservoir = new byte[RESERVOIR_LENGTH];

		private int pos = reservoir.Length;

		private void fillReservoir(int min)
		{
			if (pos >= reservoir.Length - min)
			{
				try
				{
					if (stream == null)
					{
						stream = new java.io.FileInputStream(new java.io.File(randomDevPath));
					}
					org.apache.hadoop.io.IOUtils.readFully(stream, reservoir, 0, reservoir.Length);
				}
				catch (System.IO.IOException e)
				{
					throw new System.Exception("failed to fill reservoir", e);
				}
				pos = 0;
			}
		}

		public OsSecureRandom()
		{
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			lock (this)
			{
				this.conf = conf;
				this.randomDevPath = conf.get(org.apache.hadoop.fs.CommonConfigurationKeysPublic.
					HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);
				close();
			}
		}

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			lock (this)
			{
				return conf;
			}
		}

		public override void nextBytes(byte[] bytes)
		{
			lock (this)
			{
				int off = 0;
				int n = 0;
				while (off < bytes.Length)
				{
					fillReservoir(0);
					n = System.Math.min(bytes.Length - off, reservoir.Length - pos);
					System.Array.Copy(reservoir, pos, bytes, off, n);
					off += n;
					pos += n;
				}
			}
		}

		protected override int next(int nbits)
		{
			lock (this)
			{
				fillReservoir(4);
				int n = 0;
				for (int i = 0; i < 4; i++)
				{
					n = ((n << 8) | (reservoir[pos++] & unchecked((int)(0xff))));
				}
				return n & (unchecked((int)(0xffffffff)) >> (32 - nbits));
			}
		}

		public virtual void close()
		{
			lock (this)
			{
				if (stream != null)
				{
					org.apache.hadoop.io.IOUtils.cleanup(LOG, stream);
					stream = null;
				}
			}
		}
	}
}
