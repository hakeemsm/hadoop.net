using Sharpen;

namespace org.apache.hadoop.io.compress
{
	/// <summary>
	/// A global compressor/decompressor pool used to save and reuse
	/// (possibly native) compression/decompression codecs.
	/// </summary>
	public class CodecPool
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.CodecPool
			)));

		/// <summary>
		/// A global compressor pool used to save the expensive
		/// construction/destruction of (possibly native) decompression codecs.
		/// </summary>
		private static readonly System.Collections.Generic.IDictionary<java.lang.Class, System.Collections.Generic.ICollection
			<org.apache.hadoop.io.compress.Compressor>> compressorPool = new System.Collections.Generic.Dictionary
			<java.lang.Class, System.Collections.Generic.ICollection<org.apache.hadoop.io.compress.Compressor
			>>();

		/// <summary>
		/// A global decompressor pool used to save the expensive
		/// construction/destruction of (possibly native) decompression codecs.
		/// </summary>
		private static readonly System.Collections.Generic.IDictionary<java.lang.Class, System.Collections.Generic.ICollection
			<org.apache.hadoop.io.compress.Decompressor>> decompressorPool = new System.Collections.Generic.Dictionary
			<java.lang.Class, System.Collections.Generic.ICollection<org.apache.hadoop.io.compress.Decompressor
			>>();

		private static com.google.common.cache.LoadingCache<java.lang.Class, java.util.concurrent.atomic.AtomicInteger
			> createCache<T>()
		{
			System.Type klass = typeof(T);
			return com.google.common.cache.CacheBuilder.newBuilder().build(new _CacheLoader_63
				());
		}

		private sealed class _CacheLoader_63 : com.google.common.cache.CacheLoader<java.lang.Class
			, java.util.concurrent.atomic.AtomicInteger>
		{
			public _CacheLoader_63()
			{
			}

			/// <exception cref="System.Exception"/>
			public override java.util.concurrent.atomic.AtomicInteger load(java.lang.Class key
				)
			{
				return new java.util.concurrent.atomic.AtomicInteger();
			}
		}

		/// <summary>Map to track the number of leased compressors</summary>
		private static readonly com.google.common.cache.LoadingCache<java.lang.Class, java.util.concurrent.atomic.AtomicInteger
			> compressorCounts = createCache<org.apache.hadoop.io.compress.Compressor>();

		/// <summary>Map to tracks the number of leased decompressors</summary>
		private static readonly com.google.common.cache.LoadingCache<java.lang.Class, java.util.concurrent.atomic.AtomicInteger
			> decompressorCounts = createCache<org.apache.hadoop.io.compress.Decompressor>();

		private static T borrow<T>(System.Collections.Generic.IDictionary<java.lang.Class
			, System.Collections.Generic.ICollection<T>> pool, java.lang.Class codecClass)
		{
			T codec = null;
			// Check if an appropriate codec is available
			System.Collections.Generic.ICollection<T> codecSet;
			lock (pool)
			{
				codecSet = pool[codecClass];
			}
			if (codecSet != null)
			{
				lock (codecSet)
				{
					if (!codecSet.isEmpty())
					{
						codec = codecSet.GetEnumerator().Current;
						codecSet.remove(codec);
					}
				}
			}
			return codec;
		}

		private static bool payback<T>(System.Collections.Generic.IDictionary<java.lang.Class
			, System.Collections.Generic.ICollection<T>> pool, T codec)
		{
			if (codec != null)
			{
				java.lang.Class codecClass = org.apache.hadoop.util.ReflectionUtils.getClass(codec
					);
				System.Collections.Generic.ICollection<T> codecSet;
				lock (pool)
				{
					codecSet = pool[codecClass];
					if (codecSet == null)
					{
						codecSet = new java.util.HashSet<T>();
						pool[codecClass] = codecSet;
					}
				}
				lock (codecSet)
				{
					return codecSet.add(codec);
				}
			}
			return false;
		}

		private static int getLeaseCount<T>(com.google.common.cache.LoadingCache<java.lang.Class
			, java.util.concurrent.atomic.AtomicInteger> usageCounts, java.lang.Class codecClass
			)
		{
			return usageCounts.getUnchecked((java.lang.Class)codecClass).get();
		}

		private static void updateLeaseCount<T>(com.google.common.cache.LoadingCache<java.lang.Class
			, java.util.concurrent.atomic.AtomicInteger> usageCounts, T codec, int delta)
		{
			if (codec != null)
			{
				java.lang.Class codecClass = org.apache.hadoop.util.ReflectionUtils.getClass(codec
					);
				usageCounts.getUnchecked(codecClass).addAndGet(delta);
			}
		}

		/// <summary>
		/// Get a
		/// <see cref="Compressor"/>
		/// for the given
		/// <see cref="CompressionCodec"/>
		/// from the
		/// pool or a new one.
		/// </summary>
		/// <param name="codec">
		/// the <code>CompressionCodec</code> for which to get the
		/// <code>Compressor</code>
		/// </param>
		/// <param name="conf">the <code>Configuration</code> object which contains confs for creating or reinit the compressor
		/// 	</param>
		/// <returns>
		/// <code>Compressor</code> for the given
		/// <code>CompressionCodec</code> from the pool or a new one
		/// </returns>
		public static org.apache.hadoop.io.compress.Compressor getCompressor(org.apache.hadoop.io.compress.CompressionCodec
			 codec, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.io.compress.Compressor compressor = borrow(compressorPool, codec
				.getCompressorType());
			if (compressor == null)
			{
				compressor = codec.createCompressor();
				LOG.info("Got brand-new compressor [" + codec.getDefaultExtension() + "]");
			}
			else
			{
				compressor.reinit(conf);
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Got recycled compressor");
				}
			}
			updateLeaseCount(compressorCounts, compressor, 1);
			return compressor;
		}

		public static org.apache.hadoop.io.compress.Compressor getCompressor(org.apache.hadoop.io.compress.CompressionCodec
			 codec)
		{
			return getCompressor(codec, null);
		}

		/// <summary>
		/// Get a
		/// <see cref="Decompressor"/>
		/// for the given
		/// <see cref="CompressionCodec"/>
		/// from the
		/// pool or a new one.
		/// </summary>
		/// <param name="codec">
		/// the <code>CompressionCodec</code> for which to get the
		/// <code>Decompressor</code>
		/// </param>
		/// <returns>
		/// <code>Decompressor</code> for the given
		/// <code>CompressionCodec</code> the pool or a new one
		/// </returns>
		public static org.apache.hadoop.io.compress.Decompressor getDecompressor(org.apache.hadoop.io.compress.CompressionCodec
			 codec)
		{
			org.apache.hadoop.io.compress.Decompressor decompressor = borrow(decompressorPool
				, codec.getDecompressorType());
			if (decompressor == null)
			{
				decompressor = codec.createDecompressor();
				LOG.info("Got brand-new decompressor [" + codec.getDefaultExtension() + "]");
			}
			else
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Got recycled decompressor");
				}
			}
			updateLeaseCount(decompressorCounts, decompressor, 1);
			return decompressor;
		}

		/// <summary>
		/// Return the
		/// <see cref="Compressor"/>
		/// to the pool.
		/// </summary>
		/// <param name="compressor">the <code>Compressor</code> to be returned to the pool</param>
		public static void returnCompressor(org.apache.hadoop.io.compress.Compressor compressor
			)
		{
			if (compressor == null)
			{
				return;
			}
			// if the compressor can't be reused, don't pool it.
			if (Sharpen.Runtime.getClassForObject(compressor).isAnnotationPresent(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.compress.DoNotPool))))
			{
				return;
			}
			compressor.reset();
			if (payback(compressorPool, compressor))
			{
				updateLeaseCount(compressorCounts, compressor, -1);
			}
		}

		/// <summary>
		/// Return the
		/// <see cref="Decompressor"/>
		/// to the pool.
		/// </summary>
		/// <param name="decompressor">
		/// the <code>Decompressor</code> to be returned to the
		/// pool
		/// </param>
		public static void returnDecompressor(org.apache.hadoop.io.compress.Decompressor 
			decompressor)
		{
			if (decompressor == null)
			{
				return;
			}
			// if the decompressor can't be reused, don't pool it.
			if (Sharpen.Runtime.getClassForObject(decompressor).isAnnotationPresent(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.compress.DoNotPool))))
			{
				return;
			}
			decompressor.reset();
			if (payback(decompressorPool, decompressor))
			{
				updateLeaseCount(decompressorCounts, decompressor, -1);
			}
		}

		/// <summary>
		/// Return the number of leased
		/// <see cref="Compressor"/>
		/// s for this
		/// <see cref="CompressionCodec"/>
		/// </summary>
		public static int getLeasedCompressorsCount(org.apache.hadoop.io.compress.CompressionCodec
			 codec)
		{
			return (codec == null) ? 0 : getLeaseCount(compressorCounts, codec.getCompressorType
				());
		}

		/// <summary>
		/// Return the number of leased
		/// <see cref="Decompressor"/>
		/// s for this
		/// <see cref="CompressionCodec"/>
		/// </summary>
		public static int getLeasedDecompressorsCount(org.apache.hadoop.io.compress.CompressionCodec
			 codec)
		{
			return (codec == null) ? 0 : getLeaseCount(decompressorCounts, codec.getDecompressorType
				());
		}
	}
}
